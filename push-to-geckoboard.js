const { Client: PgClient } = require('pg');
const Geckoboard = require('geckoboard');

// You'll need to get your API key from Geckoboard settings
const gb = new Geckoboard.GB(e118196dc4a88829997c16d7b2fa09be);
const pgClient = new PgClient({ 
  connectionString: 'postgresql://neondb_owner:npg_5eXxOfA7LbFE@ep-lucky-cake-afmdhgby-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require' 
});

async function pushDashboardMetrics() {
  try {
    await pgClient.connect();
    console.log('📊 Pushing metrics to Geckoboard...');
    
    // SCREEN 1: INSTALLATIONS
    
    // Today's Performance - Number Widget
    const todayData = await pgClient.query('SELECT * FROM v_installs_today');
    const today = todayData.rows[0];
    
    await gb.datasets.findOrCreate('installs.today.score', {
      fields: {
        completed: { type: 'number', name: 'Completed' },
        scheduled: { type: 'number', name: 'Scheduled' },
        rate: { type: 'percentage', name: 'Completion Rate' }
      }
    }).put([{
      completed: parseInt(today.deals_completed_today) || 0,
      scheduled: parseInt(today.deals_scheduled_today) || 0,
      rate: today.deals_scheduled_today > 0 ? 
        (today.deals_completed_today / today.deals_scheduled_today) : 0
    }]);
    
    // Weekly Progress - Gauge Widget
    const weekData = await pgClient.query(`
      SELECT 
        COUNT(*) FILTER (WHERE install_scheduled_date >= DATE_TRUNC('week', CURRENT_DATE)) as scheduled,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE)) as completed,
        ROUND(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE)), 1) as kw
      FROM projects
    `);
    
    await gb.datasets.findOrCreate('installs.week.progress', {
      fields: {
        value: { type: 'percentage', name: 'Weekly Completion' },
        completed: { type: 'number', name: 'Completed' },
        total: { type: 'number', name: 'Total Scheduled' }
      }
    }).put([{
      value: weekData.rows[0].scheduled > 0 ? 
        (weekData.rows[0].completed / weekData.rows[0].scheduled) : 0,
      completed: parseInt(weekData.rows[0].completed),
      total: parseInt(weekData.rows[0].scheduled)
    }]);
    
    // Daily Trend - Line Chart
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
    
    await gb.datasets.findOrCreate('installs.daily.trend', {
      fields: {
        date: { type: 'date', name: 'Date' },
        installs: { type: 'number', name: 'Installs Completed' }
      }
    }).put(trendData.rows.map(row => ({
      date: row.date.toISOString().split('T')[0],
      installs: parseInt(row.installs)
    })));
    
    // State Performance - Leaderboard
    const stateData = await pgClient.query(`
      SELECT 
        state,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('month', CURRENT_DATE)) as completed,
        ROUND(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('month', CURRENT_DATE)), 1) as kw
      FROM projects
      WHERE state IS NOT NULL
      GROUP BY state
      ORDER BY completed DESC
      LIMIT 5
    `);
    
    await gb.datasets.findOrCreate('installs.by.state', {
      fields: {
        label: { type: 'string', name: 'State' },
        value: { type: 'number', name: 'Installs' },
        kw: { type: 'number', name: 'kW' }
      }
    }).put(stateData.rows.map(row => ({
      label: row.state,
      value: parseInt(row.completed),
      kw: parseFloat(row.kw) || 0
    })));
    
    // SCREEN 2: CUSTOMER EXPERIENCE
    
    // Monthly Sales vs Completions - Bar Chart
    const monthlyData = await pgClient.query(`
      WITH months AS (
        SELECT generate_series(
          DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months'),
          DATE_TRUNC('month', CURRENT_DATE),
          '1 month'
        ) AS month
      ),
      sales AS (
        SELECT DATE_TRUNC('month', sale_date) as month, COUNT(*) as count
        FROM projects WHERE sale_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', sale_date)
      ),
      completions AS (
        SELECT DATE_TRUNC('month', install_completed_date) as month, COUNT(*) as count
        FROM projects WHERE install_completed_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', install_completed_date)
      )
      SELECT 
        TO_CHAR(m.month, 'Mon') as month_name,
        COALESCE(s.count, 0) as sales,
        COALESCE(c.count, 0) as completions
      FROM months m
      LEFT JOIN sales s ON m.month = s.month
      LEFT JOIN completions c ON m.month = c.month
      ORDER BY m.month
    `);
    
    await gb.datasets.findOrCreate('monthly.sales.completions', {
      fields: {
        month: { type: 'string', name: 'Month' },
        sales: { type: 'number', name: 'Sales' },
        completions: { type: 'number', name: 'Completions' }
      }
    }).put(monthlyData.rows.map(row => ({
      month: row.month_name,
      sales: parseInt(row.sales),
      completions: parseInt(row.completions)
    })));
    
    // SCREEN 3: OPERATIONAL PIPELINE
    
    // Pipeline Status - Funnel
    const pipelineData = await pgClient.query(`
      SELECT
        COUNT(*) as total_projects,
        COUNT(engineering_completed_date) as engineering_done,
        COUNT(permit_approved_date) as permits_done,
        COUNT(install_scheduled_date) as scheduled,
        COUNT(install_completed_date) as installed,
        COUNT(passing_inspection_completed_date) as inspected,
        COUNT(pto_approved_date) as pto_complete
      FROM projects
      WHERE sale_date >= CURRENT_DATE - 90
    `);
    
    const pipeline = pipelineData.rows[0];
    await gb.datasets.findOrCreate('pipeline.funnel', {
      fields: {
        stage: { type: 'string', name: 'Stage' },
        count: { type: 'number', name: 'Projects' }
      }
    }).put([
      { stage: 'Sold', count: parseInt(pipeline.total_projects) },
      { stage: 'Engineering', count: parseInt(pipeline.engineering_done) },
      { stage: 'Permit', count: parseInt(pipeline.permits_done) },
      { stage: 'Scheduled', count: parseInt(pipeline.scheduled) },
      { stage: 'Installed', count: parseInt(pipeline.installed) },
      { stage: 'Inspected', count: parseInt(pipeline.inspected) },
      { stage: 'PTO', count: parseInt(pipeline.pto_complete) }
    ]);
    
    console.log('✅ All metrics pushed to Geckoboard!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pgClient.end();
  }
}

// Run immediately
pushDashboardMetrics();

// Schedule updates every 5 minutes
if (process.env.RAILWAY_ENVIRONMENT || process.argv.includes('--continuous')) {
  setInterval(pushDashboardMetrics, 5 * 60 * 1000);
  console.log('🔄 Updating Geckoboard every 5 minutes...');
}