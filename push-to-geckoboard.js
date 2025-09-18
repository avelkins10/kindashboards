const { Client: PgClient } = require('pg');

// Geckoboard uses a different import for CommonJS
const geckoboard = require('geckoboard');
const gb = new geckoboard.Geckoboard('e118196dc4a88829997c16d7b2fa09be');

const pgClient = new PgClient({ 
  connectionString: 'postgresql://neondb_owner:npg_5eXxOfA7LbFE@ep-lucky-cake-afmdhgby-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require' 
});

async function pushDashboardMetrics() {
  try {
    await pgClient.connect();
    console.log('📊 Pushing metrics to Geckoboard...');
    
    // Define and create dataset for today's installs
    const todayDataset = gb.defineDataset({
      id: 'installs.today',
      fields: {
        scheduled: { type: 'number', name: 'Scheduled' },
        completed: { type: 'number', name: 'Completed' },
        kw: { type: 'number', name: 'kW Completed' }
      }
    });
    
    await todayDataset.create();
    
    // Get and push today's data
    const todayData = await pgClient.query('SELECT * FROM v_installs_today');
    const today = todayData.rows[0];
    
   // Around line 30, update to:
    await todayDataset.replace([{
  scheduled: parseInt(today.deals_scheduled_today) || 0,
  completed: parseInt(today.deals_completed_today) || 0,
  kw: parseFloat(today.kw_completed_today) || 0
    }]);
    
    // Weekly dataset
    const weekDataset = gb.defineDataset({
      id: 'installs.week',
      fields: {
        scheduled: { type: 'number', name: 'Week Scheduled' },
        completed: { type: 'number', name: 'Week Completed' },
        kw: { type: 'number', name: 'kW This Week' }
      }
    });
    
    await weekDataset.create();
    
    const weekData = await pgClient.query(`
      SELECT 
        COUNT(*) FILTER (WHERE install_scheduled_date >= DATE_TRUNC('week', CURRENT_DATE))::INTEGER as scheduled,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE))::INTEGER as completed,
        COALESCE(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE)), 0)::FLOAT as kw
      FROM projects
    `);
    
    // And ensure the replace line converts properly:
    await weekDataset.replace([{
      scheduled: parseInt(weekData.rows[0].scheduled) || 0,
      completed: parseInt(weekData.rows[0].completed) || 0,
      kw: parseFloat(weekData.rows[0].kw) || 0
    }]);
    
    // Daily trend dataset
    const trendDataset = gb.defineDataset({
      id: 'installs.trend',
      fields: {
        date: { type: 'date', name: 'Date' },
        installs: { type: 'number', name: 'Installs' }
      }
    });
    
    await trendDataset.create();
    
    const trendData = await pgClient.query(`
      SELECT 
        DATE(install_completed_date) as date,
        COUNT(*) as installs
      FROM projects
      WHERE install_completed_date >= CURRENT_DATE - 14
        AND install_completed_date <= CURRENT_DATE
      GROUP BY DATE(install_completed_date)
      ORDER BY date
    `);
    
    await trendDataset.replace(trendData.rows.map(row => ({
      date: row.date.toISOString().split('T')[0],
      installs: parseInt(row.installs)
    })));
    
    console.log('✅ All metrics pushed to Geckoboard!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pgClient.end();
  }
}

// Run immediately
pushDashboardMetrics();

// Schedule updates
if (process.env.RAILWAY_ENVIRONMENT || process.argv.includes('--continuous')) {
  setInterval(pushDashboardMetrics, 5 * 60 * 1000);
  console.log('🔄 Updating Geckoboard every 5 minutes...');
}