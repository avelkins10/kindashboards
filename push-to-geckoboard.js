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
    
    // TODAY'S INSTALLS
    const todayDataset = gb.defineDataset({
      id: 'installs.today',
      fields: {
        scheduled: { type: 'number', name: 'Scheduled' },
        completed: { type: 'number', name: 'Completed' },
        kw: { type: 'number', name: 'kW Completed' }
      }
    });
    
    await todayDataset.create();
    
    const todayData = await pgClient.query('SELECT * FROM v_installs_today');
    const today = todayData.rows[0];
    
    await todayDataset.replace([{
      scheduled: parseInt(today.deals_scheduled_today) || 0,
      completed: parseInt(today.deals_completed_today) || 0,
      kw: parseFloat(today.kw_completed_today) || 0
    }]);
    
    // WEEKLY PROGRESS
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
    
    await weekDataset.replace([{
      scheduled: parseInt(weekData.rows[0].scheduled) || 0,
      completed: parseInt(weekData.rows[0].completed) || 0,
      kw: parseFloat(weekData.rows[0].kw) || 0
    }]);
    
    // DAILY TREND
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
    
    // MONTHLY KW PROGRESS
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
        COALESCE(s.kw_sold, 0)::FLOAT as sales_kw,
        COALESCE(c.kw_completed, 0)::FLOAT as completed_kw,
        1000::FLOAT as target_kw
      FROM months m
      LEFT JOIN sales s ON m.month = s.month
      LEFT JOIN completions c ON m.month = c.month
      ORDER BY m.month
    `);
    
    await monthlyDataset.replace(monthlyData.rows);
    
    // CURRENT MONTH PERFORMANCE
    const monthlyInstallsDataset = gb.defineDataset({
      id: 'monthly.current',
      fields: {
        month: { type: 'string', name: 'Month' },
        scheduled: { type: 'number', name: 'Scheduled' },
        completed: { type: 'number', name: 'Completed' }
      }
    });
    
    await monthlyInstallsDataset.create();
    
    const monthlyInstalls = await pgClient.query(`
      SELECT 
        TO_CHAR(CURRENT_DATE, 'Month YYYY') as month,
        COUNT(*) FILTER (WHERE install_scheduled_date >= DATE_TRUNC('month', CURRENT_DATE) 
          AND install_scheduled_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')::INTEGER as scheduled,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('month', CURRENT_DATE))::INTEGER as completed
      FROM projects
    `);
    
    await monthlyInstallsDataset.replace([monthlyInstalls.rows[0]]);
    
    // STATE LEADERBOARD
    const stateDataset = gb.defineDataset({
      id: 'performance.by.state',
      fields: {
        state: { type: 'string', name: 'State' },
        installs: { type: 'number', name: 'Installs This Month' },
        kw: { type: 'number', name: 'kW Installed' }
      }
    });
    
    await stateDataset.create();
    
    const stateData = await pgClient.query(`
      SELECT 
        state,
        COUNT(*)::INTEGER as installs,
        ROUND(SUM(system_size_kw)::NUMERIC, 1)::FLOAT as kw
      FROM projects
      WHERE install_completed_date >= DATE_TRUNC('month', CURRENT_DATE)
        AND state IS NOT NULL
      GROUP BY state
      ORDER BY COUNT(*) DESC
      LIMIT 7
    `);
    
    await stateDataset.replace(stateData.rows);
    
    // PIPELINE FUNNEL
    const pipelineDataset = gb.defineDataset({
      id: 'pipeline.funnel',
      fields: {
        stage: { type: 'string', name: 'Stage' },
        count: { type: 'number', name: 'Projects' }
      }
    });
    
    await pipelineDataset.create();
    
    const pipelineData = await pgClient.query(`
      SELECT
        COUNT(*)::INTEGER as total,
        COUNT(engineering_completed_date)::INTEGER as engineering,
        COUNT(permit_approved_date)::INTEGER as permits,
        COUNT(install_scheduled_date)::INTEGER as scheduled,
        COUNT(install_completed_date)::INTEGER as installed,
        COUNT(passing_inspection_completed_date)::INTEGER as inspected,
        COUNT(pto_approved_date)::INTEGER as pto
      FROM projects
      WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
    `);
    
    const p = pipelineData.rows[0];
    await pipelineDataset.replace([
      { stage: '1. Sold', count: p.total },
      { stage: '2. Engineering', count: p.engineering },
      { stage: '3. Permits', count: p.permits },
      { stage: '4. Scheduled', count: p.scheduled },
      { stage: '5. Installed', count: p.installed },
      { stage: '6. Inspected', count: p.inspected },
      { stage: '7. PTO', count: p.pto }
    ]);
    
    // OVERDUE INSTALLS
    const overdueDataset = gb.defineDataset({
      id: 'installs.overdue',
      fields: {
        count: { type: 'number', name: 'Overdue Installs' },
        kw: { type: 'number', name: 'kW Overdue' },
        oldest_days: { type: 'number', name: 'Days Overdue (Oldest)' }
      }
    });
    
    await overdueDataset.create();
    
    const overdueData = await pgClient.query(`
      SELECT 
        COUNT(*)::INTEGER as count,
        COALESCE(ROUND(SUM(system_size_kw)::NUMERIC, 1), 0)::FLOAT as kw,
        COALESCE(MAX(CURRENT_DATE - install_scheduled_date), 0)::INTEGER as oldest_days
      FROM projects
      WHERE install_scheduled_date < CURRENT_DATE
        AND install_completed_date IS NULL
    `);
    
    await overdueDataset.replace([overdueData.rows[0]]);
    
    // WEEKLY COMPARISON (This Week vs Last Week)
    const weekComparisonDataset = gb.defineDataset({
      id: 'week.comparison',
      fields: {
        period: { type: 'string', name: 'Period' },
        installs: { type: 'number', name: 'Installs' },
        kw: { type: 'number', name: 'kW' }
      }
    });
    
    await weekComparisonDataset.create();
    
    const weekComparison = await pgClient.query(`
      SELECT 
        'This Week' as period,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE))::INTEGER as installs,
        COALESCE(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE)), 0)::FLOAT as kw
      FROM projects
      UNION ALL
      SELECT 
        'Last Week' as period,
        COUNT(*) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week') 
          AND install_completed_date < DATE_TRUNC('week', CURRENT_DATE))::INTEGER as installs,
        COALESCE(SUM(system_size_kw) FILTER (WHERE install_completed_date >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week')
          AND install_completed_date < DATE_TRUNC('week', CURRENT_DATE)), 0)::FLOAT as kw
      FROM projects
    `);
    
    await weekComparisonDataset.replace(weekComparison.rows);
    
    // SALES TO INSTALL CYCLE TIME
    const cycleTimeDataset = gb.defineDataset({
      id: 'cycle.time',
      fields: {
        metric: { type: 'string', name: 'Metric' },
        days: { type: 'number', name: 'Days' }
      }
    });
    
    await cycleTimeDataset.create();
    
    const cycleTime = await pgClient.query(`
      SELECT 
        'Avg Sale to Install' as metric,
        ROUND(AVG(install_completed_date - sale_date))::INTEGER as days
      FROM projects
      WHERE install_completed_date IS NOT NULL
        AND sale_date IS NOT NULL
        AND install_completed_date >= CURRENT_DATE - INTERVAL '30 days'
      UNION ALL
      SELECT 
        'Avg Sale to Schedule' as metric,
        ROUND(AVG(install_scheduled_date - sale_date))::INTEGER as days
      FROM projects
      WHERE install_scheduled_date IS NOT NULL
        AND sale_date IS NOT NULL
        AND install_scheduled_date >= CURRENT_DATE - INTERVAL '30 days'
    `);
    
    await cycleTimeDataset.replace(cycleTime.rows);
    
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
// CLOSER LEADERBOARD - CURRENT MONTH
const closerDataset = gb.defineDataset({
  id: 'leaderboard.closers',
  fields: {
    name: { type: 'string', name: 'Closer' },
    deals: { type: 'number', name: 'Deals' },
    kw: { type: 'number', name: 'kW Sold' },
    avg_size: { type: 'number', name: 'Avg System Size' }
  }
});

await closerDataset.create();

const closerData = await pgClient.query(`
  SELECT 
    COALESCE(closer_name, 'Unassigned') as name,
    COUNT(*)::INTEGER as deals,
    ROUND(SUM(system_size_kw)::NUMERIC, 1)::FLOAT as kw,
    ROUND(AVG(system_size_kw)::NUMERIC, 1)::FLOAT as avg_size
  FROM projects
  WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
    AND closer_name IS NOT NULL
  GROUP BY closer_name
  ORDER BY SUM(system_size_kw) DESC
  LIMIT 10
`);

await closerDataset.replace(closerData.rows);

// SETTER LEADERBOARD - CURRENT MONTH
const setterDataset = gb.defineDataset({
  id: 'leaderboard.setters',
  fields: {
    name: { type: 'string', name: 'Setter' },
    deals: { type: 'number', name: 'Deals Set' },
    kw: { type: 'number', name: 'kW Set' }
  }
});

await setterDataset.create();

const setterData = await pgClient.query(`
  SELECT 
    COALESCE(setter_name, 'Unassigned') as name,
    COUNT(*)::INTEGER as deals,
    ROUND(SUM(system_size_kw)::NUMERIC, 1)::FLOAT as kw
  FROM projects
  WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
    AND setter_name IS NOT NULL
  GROUP BY setter_name
  ORDER BY COUNT(*) DESC
  LIMIT 10
`);

await setterDataset.replace(setterData.rows);

// SALES OFFICE PERFORMANCE
const officeDataset = gb.defineDataset({
  id: 'performance.by.office',
  fields: {
    office: { type: 'string', name: 'Office' },
    sales: { type: 'number', name: 'Sales' },
    installs: { type: 'number', name: 'Installs' },
    conversion: { type: 'percentage', name: 'Install Rate' }
  }
});

await officeDataset.create();

const officeData = await pgClient.query(`
  SELECT 
    COALESCE(sales_office, 'Unknown') as office,
    COUNT(*)::INTEGER as sales,
    COUNT(install_completed_date)::INTEGER as installs,
    CASE 
      WHEN COUNT(*) > 0 
      THEN (COUNT(install_completed_date)::FLOAT / COUNT(*)::FLOAT)
      ELSE 0 
    END as conversion
  FROM projects
  WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY sales_office
  ORDER BY COUNT(*) DESC
`);

await officeDataset.replace(officeData.rows);

// TOP PERFORMERS THIS WEEK
const weeklyTopDataset = gb.defineDataset({
  id: 'top.performers.week',
  fields: {
    name: { type: 'string', name: 'Rep' },
    role: { type: 'string', name: 'Role' },
    metric: { type: 'number', name: 'Deals' }
  }
});

await weeklyTopDataset.create();

const weeklyTop = await pgClient.query(`
  WITH closers AS (
    SELECT 
      closer_name as name,
      'Closer' as role,
      COUNT(*) as metric
    FROM projects
    WHERE sale_date >= DATE_TRUNC('week', CURRENT_DATE)
      AND closer_name IS NOT NULL
    GROUP BY closer_name
  ),
  setters AS (
    SELECT 
      setter_name as name,
      'Setter' as role,
      COUNT(*) as metric
    FROM projects
    WHERE sale_date >= DATE_TRUNC('week', CURRENT_DATE)
      AND setter_name IS NOT NULL
    GROUP BY setter_name
  )
  SELECT * FROM (
    SELECT * FROM closers
    UNION ALL
    SELECT * FROM setters
  ) combined
  ORDER BY metric DESC
  LIMIT 5
`);

await weeklyTopDataset.replace(weeklyTop.rows.map(row => ({
  name: row.name,
  role: row.role,
  metric: parseInt(row.metric)
})));

// CLOSER CONVERSION RATES
const conversionDataset = gb.defineDataset({
  id: 'closer.conversion',
  fields: {
    name: { type: 'string', name: 'Closer' },
    sold: { type: 'number', name: 'Sold' },
    installed: { type: 'number', name: 'Installed' },
    rate: { type: 'percentage', name: 'Install Rate' }
  }
});

await conversionDataset.create();

const conversionData = await pgClient.query(`
  SELECT 
    closer_name as name,
    COUNT(*)::INTEGER as sold,
    COUNT(install_completed_date)::INTEGER as installed,
    CASE 
      WHEN COUNT(*) > 0 
      THEN (COUNT(install_completed_date)::FLOAT / COUNT(*)::FLOAT)
      ELSE 0 
    END as rate
  FROM projects
  WHERE sale_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months'
    AND closer_name IS NOT NULL
  GROUP BY closer_name
  HAVING COUNT(*) >= 5  -- Only show closers with at least 5 sales
  ORDER BY (COUNT(install_completed_date)::FLOAT / COUNT(*)::FLOAT) DESC
`);

await conversionDataset.replace(conversionData.rows);
