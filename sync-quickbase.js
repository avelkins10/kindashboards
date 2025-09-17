const { Client } = require('pg');
const axios = require('axios');

// YOUR ACTUAL CONFIG
const CONFIG = {
  // Quickbase credentials
  QB_REALM: 'kin.quickbase.com',
  QB_TOKEN: 'b6um6p_p3bs_0_du6x8cjdnz3jybdcztziiqda3us',
  QB_TABLE_ID: 'br9kwm8na',
  
  // Your Neon database
  NEON_URL: 'postgresql://neondb_owner:npg_5eXxOfA7LbFE@ep-lucky-cake-afmdhgby-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require'
};

// Field mappings - ADDED SALES DATE
const FIELD_IDS = {
  recordId: 3,
  customerName: 145,
  customerEmail: 149,
  state: 189,
  systemSize: 13,
  status: 255,
  salesDate: 522,  // ADDED THIS
  installScheduled: 1356,
  installCompleted: 534,
  engineeringCompleted: 478,
  permitApproved: 208,
  inspectionPassed: 491,
  ptoApproved: 538,
  closerName: 517,
  setterName: 330,
  salesOffice: 339,
  projectCoordinator: 820
};

async function syncData(isBackfill = false) {
  const pgClient = new Client({ connectionString: CONFIG.NEON_URL });
  
  try {
    await pgClient.connect();
    console.log('✅ Connected to Neon');
    
    // First, let's add the sales_date column if it doesn't exist
    await pgClient.query(`
      ALTER TABLE projects 
      ADD COLUMN IF NOT EXISTS sale_date DATE;
    `);
    console.log('✅ Database schema updated');
    
    // Build the where clause based on whether we're backfilling
    let whereClause;
    
    if (isBackfill) {
      // Get ALL records sold since April 28, 2024
      console.log('📥 BACKFILLING all sales since April 28, 2024...');
      whereClause = "{522.OAF.'04-28-2024'}"; // Sales date on or after April 28
    } else {
      console.log('📥 Fetching recent sales from Kin Quickbase...');
      // Just get last 90 days for regular syncs
      const ninetyDaysAgo = new Date();
      ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
      const dateString = `${(ninetyDaysAgo.getMonth() + 1).toString().padStart(2, '0')}-${ninetyDaysAgo.getDate().toString().padStart(2, '0')}-${ninetyDaysAgo.getFullYear()}`;
      whereClause = `{522.OAF.'${dateString}'}`; // Sales date in last 90 days
    }
    
    // Fetch from Quickbase
    const response = await axios.post(
      'https://api.quickbase.com/v1/records/query',
      {
        from: CONFIG.QB_TABLE_ID,
        select: Object.values(FIELD_IDS),
        where: whereClause,
        sortBy: [{ fieldId: 522, order: 'DESC' }], // Sort by sales date, newest first
        options: { 
          top: isBackfill ? 10000 : 1000  // Get more records for backfill
        }
      },
      {
        headers: {
          'QB-Realm-Hostname': CONFIG.QB_REALM,
          'Authorization': `QB-USER-TOKEN ${CONFIG.QB_TOKEN}`,
          'Content-Type': 'application/json'
        }
      }
    );
    
    console.log(`📊 Found ${response.data.data.length} projects to sync`);
    
    // Clear test data first (only on regular sync)
    if (!isBackfill) {
      await pgClient.query("DELETE FROM projects WHERE record_id LIKE 'TEST%'");
    }
    
    // Track progress
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
    // Process in batches for better performance
    const batchSize = 100;
    for (let i = 0; i < response.data.data.length; i += batchSize) {
      const batch = response.data.data.slice(i, i + batchSize);
      console.log(`Processing batch ${Math.floor(i/batchSize) + 1} of ${Math.ceil(response.data.data.length/batchSize)}...`);
      
      for (const record of batch) {
        try {
          // Parse the sales date
          let salesDate = null;
          if (record[522]?.value) {
            salesDate = new Date(record[522].value);
          }
          
          await pgClient.query(`
            INSERT INTO projects (
              record_id, customer_name, customer_email, state, system_size_kw, status,
              sale_date, install_scheduled_date, install_completed_date, 
              engineering_completed_date, permit_approved_date, 
              passing_inspection_completed_date, pto_approved_date,
              closer_name, setter_name, sales_office, project_coordinator,
              last_synced
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
            ON CONFLICT (record_id) DO UPDATE SET
              customer_name = $2,
              customer_email = $3,
              state = $4,
              system_size_kw = $5,
              status = $6,
              sale_date = $7,
              install_scheduled_date = $8,
              install_completed_date = $9,
              engineering_completed_date = $10,
              permit_approved_date = $11,
              passing_inspection_completed_date = $12,
              pto_approved_date = $13,
              closer_name = $14,
              setter_name = $15,
              sales_office = $16,
              project_coordinator = $17,
              last_synced = NOW()
          `, [
            record[3]?.value || `QB_${Date.now()}_${i}`,  // record_id
            record[145]?.value,   // customer_name
            record[149]?.value,   // customer_email
            record[189]?.value,   // state
            parseFloat(record[13]?.value) || 0,    // system_size
            record[255]?.value,   // status
            salesDate,            // sale_date (NEW)
            record[1356]?.value ? new Date(record[1356].value) : null,  // install_scheduled
            record[534]?.value ? new Date(record[534].value) : null,    // install_completed
            record[478]?.value ? new Date(record[478].value) : null,    // engineering_completed
            record[208]?.value ? new Date(record[208].value) : null,    // permit_approved
            record[491]?.value ? new Date(record[491].value) : null,    // inspection_passed
            record[538]?.value ? new Date(record[538].value) : null,    // pto_approved
            record[517]?.value,   // closer_name
            record[330]?.value,   // setter_name
            record[339]?.value,   // sales_office
            record[820]?.value    // project_coordinator
          ]);
          successCount++;
        } catch (err) {
          errorCount++;
          errors.push(`Record ${record[3]?.value}: ${err.message}`);
        }
      }
    }
    
    console.log(`\n✅ Sync Results:`);
    console.log(`   - Successfully synced: ${successCount} projects`);
    console.log(`   - Errors: ${errorCount} projects`);
    
    if (errors.length > 0 && errors.length <= 5) {
      console.log(`\n⚠️ First few errors:`);
      errors.slice(0, 5).forEach(err => console.log(`   - ${err}`));
    }
    
    // Show summary
    console.log('\n📊 DASHBOARD SUMMARY:');
    console.log('===================');
    
    // Overall stats
    const totalStats = await pgClient.query(`
      SELECT 
        COUNT(*) as total_projects,
        COUNT(DISTINCT state) as states_active,
        ROUND(SUM(system_size_kw)::NUMERIC, 1) as total_kw,
        COUNT(install_completed_date) as total_completed,
        MIN(sale_date) as earliest_sale,
        MAX(sale_date) as latest_sale
      FROM projects
      WHERE sale_date IS NOT NULL
    `);
    
    console.log('Database Overview:', {
      total_projects: totalStats.rows[0].total_projects,
      states: totalStats.rows[0].states_active,
      total_kw: `${totalStats.rows[0].total_kw} kW`,
      completed_installs: totalStats.rows[0].total_completed,
      date_range: `${new Date(totalStats.rows[0].earliest_sale).toLocaleDateString()} to ${new Date(totalStats.rows[0].latest_sale).toLocaleDateString()}`
    });
    
    // Monthly sales summary (last 6 months)
    const monthlySales = await pgClient.query(`
      SELECT 
        DATE_TRUNC('month', sale_date) as month,
        COUNT(*) as deals_sold,
        ROUND(SUM(system_size_kw)::NUMERIC, 1) as kw_sold,
        COUNT(install_completed_date) as installs_completed
      FROM projects
      WHERE sale_date IS NOT NULL
      GROUP BY DATE_TRUNC('month', sale_date)
      ORDER BY month DESC
      LIMIT 6
    `);
    
    console.log('\nMonthly Sales Summary:');
    monthlySales.rows.forEach(row => {
      const monthName = new Date(row.month).toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
      console.log(`  ${monthName}: ${row.deals_sold} sold, ${row.kw_sold} kW, ${row.installs_completed} installed`);
    });
    
    // Today's metrics
    const installs = await pgClient.query('SELECT * FROM v_installs_today');
    console.log('\nToday\'s Installs:', {
      scheduled: `${installs.rows[0]?.deals_scheduled_today || 0} deals (${installs.rows[0]?.kw_scheduled_today || 0} kW)`,
      completed: `${installs.rows[0]?.deals_completed_today || 0} deals (${installs.rows[0]?.kw_completed_today || 0} kW)`,
      week_total: `${installs.rows[0]?.deals_completed_week || 0} deals this week`
    });
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.response) {
      console.error('API Response:', error.response.data);
    }
  } finally {
    await pgClient.end();
  }
}

// Check command line arguments
const args = process.argv.slice(2);
const isBackfill = args.includes('--backfill');

// Run it
if (isBackfill) {
  console.log('🔄 RUNNING HISTORICAL BACKFILL FROM APRIL 28, 2024');
  console.log('This will fetch all sales since April 28...\n');
  syncData(true);
} else {
  console.log('🔄 RUNNING REGULAR SYNC (Last 90 days of sales)');
  syncData(false);
}  // <-- ADD THIS CLOSING BRACKET

// For continuous running on Railway
if (process.env.RAILWAY_ENVIRONMENT || process.argv.includes('--continuous')) {
  console.log('🚂 Starting continuous sync service...');
  
  // Run immediately on startup
  console.log('Running initial sync...');
  syncData(false);
  
  // Schedule to run every 5 minutes
  setInterval(() => {
    console.log(`\n⏰ Running scheduled sync at ${new Date().toISOString()}`);
    syncData(false);
  }, 5 * 60 * 1000); // 5 minutes in milliseconds
  
  console.log('✅ Sync service is running - will update every 5 minutes');
  console.log('Press Ctrl+C to stop\n');
}