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

// Field mappings
const FIELD_IDS = {
  recordId: 3,
  customerName: 145,
  customerEmail: 149,
  state: 189,
  systemSize: 13,
  status: 255,
  salesDate: 522,
  installScheduled: 1356,
  installCompleted: 534,
  installCompletedActual: 587,  // Summary field with actual completion dates
  engineeringCompleted: 478,
  permitApproved: 208,
  inspectionPassed: 491,
  ptoApproved: 538,
  closerName: 517,
  setterName: 330,
  salesOffice: 339,
  projectCoordinator: 820
};

// Convert UTC to Mountain Time (handles both MST and MDT)
function convertToMountainTime(dateValue) {
  if (!dateValue) return null;
  
  const date = new Date(dateValue);
  
  // Determine if we're in MDT (March-November) or MST
  const month = date.getUTCMonth(); // 0-11
  const dayOfMonth = date.getUTCDate();
  const dayOfWeek = date.getUTCDay();
  
  // Rough DST calculation (2nd Sunday March - 1st Sunday November)
  let isDST = false;
  if (month > 2 && month < 10) {
    isDST = true; // April through October
  } else if (month === 2) { // March
    // Check if we're past 2nd Sunday
    const secondSunday = 14 - ((new Date(date.getUTCFullYear(), 2, 1).getDay() + 6) % 7);
    isDST = dayOfMonth > secondSunday || (dayOfMonth === secondSunday && date.getUTCHours() >= 9); // 2 AM MST = 9 AM UTC
  } else if (month === 10) { // November  
    // Check if we're before 1st Sunday
    const firstSunday = 7 - ((new Date(date.getUTCFullYear(), 10, 1).getDay() + 6) % 7);
    isDST = dayOfMonth < firstSunday || (dayOfMonth === firstSunday && date.getUTCHours() < 8); // 2 AM MDT = 8 AM UTC
  }
  
  // MDT = UTC-6, MST = UTC-7
  const offset = isDST ? 6 : 7;
  
  // Create new date adjusted for Mountain Time
  const mountainDate = new Date(date.getTime() - (offset * 60 * 60 * 1000));
  return mountainDate;
}

async function syncData(isBackfill = false) {
  const pgClient = new Client({ connectionString: CONFIG.NEON_URL });
  
  try {
    await pgClient.connect();
    console.log('✅ Connected to Neon');
    
    // Ensure sale_date column exists
    await pgClient.query(`
      ALTER TABLE projects 
      ADD COLUMN IF NOT EXISTS sale_date DATE;
    `);
    console.log('✅ Database schema updated');
    
    // Build the where clause based on whether we're backfilling
    let whereClause;
    
    if (isBackfill) {
      console.log('📥 BACKFILLING all sales since April 28, 2025...');
      whereClause = "{522.OAF.'04-28-2025'}"; // Sales date on or after April 28, 2025
    } else {
      console.log('📥 Fetching recent projects from Kin Quickbase...');
      const ninetyDaysAgo = new Date();
      ninetyDaysAgo.setDate(ninetyDaysAgo.getDate() - 90);
      const dateString = `${(ninetyDaysAgo.getMonth() + 1).toString().padStart(2, '0')}-${ninetyDaysAgo.getDate().toString().padStart(2, '0')}-${ninetyDaysAgo.getFullYear()}`;
      // Get projects that were either sold OR completed in last 90 days
      whereClause = `{522.OAF.'${dateString}'}OR{587.OAF.'${dateString}'}`; 
    }
    
    // Fetch from Quickbase
    const response = await axios.post(
      'https://api.quickbase.com/v1/records/query',
      {
        from: CONFIG.QB_TABLE_ID,
        select: Object.values(FIELD_IDS),
        where: whereClause,
        sortBy: [{ fieldId: 522, order: 'DESC' }],
        options: { 
          top: isBackfill ? 10000 : 2000
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
          // Convert all dates to Mountain Time
          const salesDate = convertToMountainTime(record[522]?.value);
          const scheduledDate = convertToMountainTime(record[1356]?.value);
          const engineeringDate = convertToMountainTime(record[478]?.value);
          const permitDate = convertToMountainTime(record[208]?.value);
          const inspectionDate = convertToMountainTime(record[491]?.value);
          const ptoDate = convertToMountainTime(record[538]?.value);
          
          // Use actual completion date (587) if available, otherwise fall back to 534
          let completionDate = null;
          if (record[587]?.value) {
            completionDate = convertToMountainTime(record[587].value);
          } else if (record[534]?.value) {
            completionDate = convertToMountainTime(record[534].value);
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
            record[3]?.value || `QB_${Date.now()}_${i}`,
            record[145]?.value,
            record[149]?.value,
            record[189]?.value,
            parseFloat(record[13]?.value) || 0,
            record[255]?.value,
            salesDate,
            scheduledDate,
            completionDate,
            engineeringDate,
            permitDate,
            inspectionDate,
            ptoDate,
            record[517]?.value,
            record[330]?.value,
            record[339]?.value,
            record[820]?.value
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
    console.log('\n📊 DASHBOARD SUMMARY (Mountain Time):');
    console.log('=====================================');
    
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
    
    // Separate queries for sales and completions by month
    console.log('\n📈 Monthly Sales & Completions:');
    
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
    
    const monthlyMetrics = await pgClient.query(`
      WITH months AS (
        SELECT generate_series(
          DATE_TRUNC('month', $1::date),
          DATE_TRUNC('month', CURRENT_DATE),
          '1 month'::interval
        ) AS month
      ),
      sales AS (
        SELECT 
          DATE_TRUNC('month', sale_date) as month,
          COUNT(*) as deals_sold,
          ROUND(SUM(system_size_kw)::NUMERIC, 1) as kw_sold
        FROM projects
        WHERE sale_date >= $1
        GROUP BY DATE_TRUNC('month', sale_date)
      ),
      completions AS (
        SELECT 
          DATE_TRUNC('month', install_completed_date) as month,
          COUNT(*) as installs_completed,
          ROUND(SUM(system_size_kw)::NUMERIC, 1) as kw_completed
        FROM projects
        WHERE install_completed_date >= $1
        GROUP BY DATE_TRUNC('month', install_completed_date)
      )
      SELECT 
        TO_CHAR(m.month, 'Month YYYY') as month_name,
        COALESCE(s.deals_sold, 0) as deals_sold,
        COALESCE(s.kw_sold, 0) as kw_sold,
        COALESCE(c.installs_completed, 0) as installs_completed,
        COALESCE(c.kw_completed, 0) as kw_completed
      FROM months m
      LEFT JOIN sales s ON m.month = s.month
      LEFT JOIN completions c ON m.month = c.month
      ORDER BY m.month DESC
      LIMIT 6
    `, [sixMonthsAgo]);
    
    monthlyMetrics.rows.forEach(row => {
      console.log(`  ${row.month_name.trim()}:`);
      console.log(`    Sales: ${row.deals_sold} deals (${row.kw_sold} kW)`);
      console.log(`    Completions: ${row.installs_completed} installs (${row.kw_completed} kW)`);
    });
    
    // Today's metrics
    const todayMetrics = await pgClient.query(`
      SELECT 
        COUNT(*) FILTER (WHERE install_scheduled_date = CURRENT_DATE) as deals_scheduled_today,
        ROUND(SUM(system_size_kw) FILTER (WHERE install_scheduled_date = CURRENT_DATE)::NUMERIC, 1) as kw_scheduled_today,
        COUNT(*) FILTER (WHERE install_completed_date = CURRENT_DATE) as deals_completed_today,
        ROUND(SUM(system_size_kw) FILTER (WHERE install_completed_date = CURRENT_DATE)::NUMERIC, 1) as kw_completed_today,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE)) as deals_completed_week,
        ROUND(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE))::NUMERIC, 1) as kw_completed_week
      FROM projects
    `);
    
    console.log('\n📅 Today\'s Metrics:', {
      scheduled: `${todayMetrics.rows[0].deals_scheduled_today} deals (${todayMetrics.rows[0].kw_scheduled_today || 0} kW)`,
      completed: `${todayMetrics.rows[0].deals_completed_today} deals (${todayMetrics.rows[0].kw_completed_today || 0} kW)`,
      week_total: `${todayMetrics.rows[0].deals_completed_week} deals (${todayMetrics.rows[0].kw_completed_week || 0} kW) this week`
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
  console.log('🔄 RUNNING HISTORICAL BACKFILL FROM APRIL 28, 2025');
  console.log('This will fetch all sales since April 28, 2025...\n');
  syncData(true);
} else {
  console.log('🔄 RUNNING REGULAR SYNC (Last 90 days)');
  syncData(false);
}

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
  }, 5 * 60 * 1000);
  
  console.log('✅ Sync service is running - will update every 5 minutes');
  console.log('Press Ctrl+C to stop\n');
}