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
// Monthly kW Progress Dataset
const monthlyDataset = gb.defineDataset({
  id: 'monthly.kw.progress',
  fields: {
    month: { type: 'string', name: 'Month' },
    sales_kw: { type: 'number', name: 'kW Sold' },
    completed_kw: { type: 'number', name: 'kW Installed' },
    target_kw: { type: 'number', name: 'Target kW' }
  }
});

await monthlyDataset.create();

const monthlyData = await pgClient.query(`
  WITH months AS (
    SELECT generate_series(
      DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months'),
      DATE_TRUNC('month', CURRENT_DATE),
      '1 month'
    ) AS month
  ),
  sales AS (
    SELECT 
      DATE_TRUNC('month', sale_date) as month,
      ROUND(SUM(system_size_kw)::NUMERIC, 1) as kw_sold
    FROM projects 
    WHERE sale_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY DATE_TRUNC('month', sale_date)
  ),
  completions AS (
    SELECT 
      DATE_TRUNC('month', install_completed_date) as month,
      ROUND(SUM(system_size_kw)::NUMERIC, 1) as kw_completed
    FROM projects 
    WHERE install_completed_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY DATE_TRUNC('month', install_completed_date)
  )
  SELECT 
    TO_CHAR(m.month, 'Mon') as month,
    COALESCE(s.kw_sold, 0) as sales_kw,
    COALESCE(c.kw_completed, 0) as completed_kw,
    1000 as target_kw  -- Set your actual monthly targets here
  FROM months m
  LEFT JOIN sales s ON m.month = s.month
  LEFT JOIN completions c ON m.month = c.month
  ORDER BY m.month
`);

await monthlyDataset.replace(monthlyData.rows);

// Monthly Install Count Dataset
const monthlyInstallsDataset = gb.defineDataset({
  id: 'monthly.installs',
  fields: {
    month: { type: 'string', name: 'Month' },
    scheduled: { type: 'number', name: 'Scheduled' },
    completed: { type: 'number', name: 'Completed' },
    completion_rate: { type: 'percentage', name: 'Completion Rate' }
  }
});

await monthlyInstallsDataset.create();

const monthlyInstalls = await pgClient.query(`
  SELECT 
    TO_CHAR(DATE_TRUNC('month', CURRENT_DATE), 'Month YYYY') as month,
    COUNT(*) FILTER (WHERE install_scheduled_date >= DATE_TRUNC('month', CURRENT_DATE) 
      AND install_scheduled_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month') as scheduled,
    COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('month', CURRENT_DATE)) as completed
  FROM projects
`);

const currentMonth = monthlyInstalls.rows[0];
await monthlyInstallsDataset.replace([{
  month: currentMonth.month,
  scheduled: parseInt(currentMonth.scheduled) || 0,
  completed: parseInt(currentMonth.completed) || 0,
  completion_rate: currentMonth.scheduled > 0 ? 
    (currentMonth.completed / currentMonth.scheduled) : 0
}]);