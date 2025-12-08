// static/insights_charts.js

function getCanvas(id) {
  const el = document.getElementById(id);
  if (!el) return null;
  return el.getContext("2d");
}

function toMonthLabel(y, m) {
  if (!y || !m) return "Unknown";
  const mm = String(m).padStart(2, "0");
  return `${y}-${mm}`;
}

// Existing charts -----------------------------------------------------

function buildSectorChart() {
  const ctx = getCanvas("sectorChart");
  if (!ctx || !stats.sector_stats) return;

  const top = stats.sector_stats.slice(0, 12);
  const labels = top.map((s) => s.sector);
  const data = top.map((s) => s.avg_rate || 0);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Average pay (£/hr)",
          data,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

function buildRoleChart() {
  const ctx = getCanvas("roleChart");
  if (!ctx || !stats.top_roles) return;

  const top = stats.top_roles.slice(0, 12);
  const labels = top.map((r) => r.role);
  const counts = top.map((r) => r.count);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Records",
          data: counts,
        },
      ],
    },
    options: {
      responsive: true,
      scales: { y: { beginAtZero: true } },
    },
  });
}

function buildCountyPieChart() {
  const ctx = getCanvas("countyPieChart");
  if (!ctx || !stats.top_counties) return;

  const top = stats.top_counties.slice(0, 10);
  const labels = top.map((c) => c.county);
  const data = top.map((c) => c.count);

  new Chart(ctx, {
    type: "pie",
    data: {
      labels,
      datasets: [
        {
          data,
        },
      ],
    },
    options: { responsive: true },
  });
}

function buildSectorPieChart() {
  const ctx = getCanvas("sectorPieChart");
  if (!ctx || !stats.sector_stats) return;

  const top = stats.sector_stats.slice(0, 10);
  const labels = top.map((s) => s.sector);
  const data = top.map((s) => s.count);

  new Chart(ctx, {
    type: "pie",
    data: {
      labels,
      datasets: [
        {
          data,
        },
      ],
    },
    options: { responsive: true },
  });
}

// New charts ----------------------------------------------------------

// 1. Pay distribution histogram
function buildPayHistogram() {
  const ctx = getCanvas("payHistogram");
  if (!ctx || !stats.distribution) return;

  const labels = stats.distribution.map((d) => d.label);
  const data = stats.distribution.map((d) => d.count || 0);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Records",
          data,
        },
      ],
    },
    options: {
      responsive: true,
      scales: { y: { beginAtZero: true } },
    },
  });
}

// 2. Pay range by sector (min/avg/max)
function buildSectorRangeChart() {
  const ctx = getCanvas("sectorRangeChart");
  if (!ctx || !stats.sector_ranges) return;

  const top = stats.sector_ranges.slice(0, 12);
  const labels = top.map((s) => s.sector);
  const minData = top.map((s) => s.min_rate || 0);
  const avgData = top.map((s) => s.avg_rate || 0);
  const maxData = top.map((s) => s.max_rate || 0);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        { label: "Min", data: minData },
        { label: "Avg", data: avgData },
        { label: "Max", data: maxData },
      ],
    },
    options: {
      responsive: true,
      scales: { y: { beginAtZero: true } },
    },
  });
}

// 3. Pay trend over time
function buildPayTrendChart() {
  const ctx = getCanvas("payTrendChart");
  if (!ctx || !stats.monthly_trend) return;

  const rows = stats.monthly_trend;
  if (!rows.length) return;

  const labels = rows.map((r) => toMonthLabel(r.year, r.month));
  const data = rows.map((r) => r.avg_rate || 0);

  new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Avg pay (£/hr)",
          data,
          tension: 0.25,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        y: { beginAtZero: false },
      },
    },
  });
}

// 4. Sector volatility (std dev)
function buildSectorVolatilityChart() {
  const ctx = getCanvas("sectorVolatilityChart");
  if (!ctx || !stats.sector_volatility) return;

  const sorted = [...stats.sector_volatility].sort(
    (a, b) => (b.stddev || 0) - (a.stddev || 0),
  );
  const top = sorted.slice(0, 12);

  const labels = top.map((s) => s.sector);
  const data = top.map((s) => s.stddev || 0);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Std dev of pay (£)",
          data,
        },
      ],
    },
    options: {
      responsive: true,
      scales: { y: { beginAtZero: true } },
    },
  });
}

// 5. Sector × county pay (stacked bar)
function buildSectorCountyHeatChart() {
  const ctx = getCanvas("sectorCountyHeatChart");
  if (!ctx || !stats.sector_county_heat) return;

  const rows = stats.sector_county_heat;
  if (!rows.length) return;

  // limit to top 6 counties by appearance
  const countyCounts = {};
  rows.forEach((r) => {
    countyCounts[r.county] = (countyCounts[r.county] || 0) + 1;
  });
  const sortedCounties = Object.entries(countyCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map((x) => x[0]);

  const sectors = [...new Set(rows.map((r) => r.sector))].slice(0, 6);

  const datasets = sectors.map((sector) => {
    const data = sortedCounties.map((county) => {
      const match = rows.find((r) => r.sector === sector && r.county === county);
      return match && match.avg_rate ? match.avg_rate : 0;
    });
    return {
      label: sector,
      data,
      stack: "stack1",
    };
  });

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: sortedCounties,
      datasets,
    },
    options: {
      responsive: true,
      scales: {
        x: { stacked: true },
        y: { stacked: true, beginAtZero: true },
      },
    },
  });
}

// 6. Top companies by pay
function buildTopCompaniesChart() {
  const ctx = getCanvas("topCompaniesChart");
  if (!ctx || !stats.top_companies) return;

  const rows = stats.top_companies;
  if (!rows.length) return;

  const labels = rows.map((r) => r.company_name);
  const data = rows.map((r) => r.avg_rate || 0);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Avg pay (£/hr)",
          data,
        },
      ],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      scales: {
        x: { beginAtZero: true },
      },
    },
  });
}

// 7. Pay outlier scatter (from records sample)
function buildOutlierScatterChart() {
  const ctx = getCanvas("outlierScatterChart");
  if (!ctx || !records || !records.length) return;

  const points = records
    .filter((r) => r.pay_rate != null && r.imported_year && r.imported_month)
    .map((r) => ({
      x: new Date(r.imported_year, (r.imported_month || 1) - 1).getTime(),
      y: r.pay_rate,
    }));

  if (!points.length) return;

  const ys = points.map((p) => p.y);
  const mean =
    ys.reduce((acc, v) => acc + v, 0) / (ys.length || 1);
  const variance =
    ys.reduce((acc, v) => acc + (v - mean) ** 2, 0) / (ys.length || 1);
  const stddev = Math.sqrt(variance);

  const thresholdLow = mean - 2 * stddev;
  const thresholdHigh = mean + 2 * stddev;

  const normal = [];
  const outliers = [];
  points.forEach((p) => {
    if (p.y < thresholdLow || p.y > thresholdHigh) {
      outliers.push(p);
    } else {
      normal.push(p);
    }
  });

  // Convert timestamps to Date for time scale
  const normalData = normal.map((p) => ({ x: new Date(p.x), y: p.y }));
  const outlierData = outliers.map((p) => ({ x: new Date(p.x), y: p.y }));

  new Chart(ctx, {
    type: "scatter",
    data: {
      datasets: [
        {
          label: "Normal",
          data: normalData,
        },
        {
          label: "Outliers",
          data: outlierData,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        x: {
          type: "time",
          time: { unit: "month" },
        },
        y: {
          beginAtZero: false,
        },
      },
    },
  });
}

// 8. Role mix by sector (stacked)
function buildRoleMixChart() {
  const ctx = getCanvas("roleMixChart");
  if (!ctx || !stats.role_mix) return;

  const rows = stats.role_mix;
  if (!rows.length) return;

  const sectors = [...new Set(rows.map((r) => r.sector))].slice(0, 6);
  const roles = [...new Set(rows.map((r) => r.role))].slice(0, 8);

  const datasets = roles.map((role) => {
    const data = sectors.map((sector) => {
      const match = rows.find((r) => r.sector === sector && r.role === role);
      return match ? match.count || 0 : 0;
    });
    return {
      label: role,
      data,
      stack: "stackRoles",
    };
  });

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: sectors,
      datasets,
    },
    options: {
      responsive: true,
      scales: {
        x: { stacked: true },
        y: { stacked: true, beginAtZero: true },
      },
    },
  });
}

// 9. County trends (line chart per county)
function buildCountyTrendChart() {
  const ctx = getCanvas("countyTrendChart");
  if (!ctx || !stats.county_trends) return;

  const entries = Object.entries(stats.county_trends);
  if (!entries.length) return;

  // Build union of all month labels
  const allPoints = [];
  entries.forEach(([county, rows]) => {
    rows.forEach((r) => allPoints.push(toMonthLabel(r.year, r.month)));
  });
  const labels = [...new Set(allPoints)].sort();

  const datasets = entries.map(([county, rows]) => {
    const map = {};
    rows.forEach((r) => {
      map[toMonthLabel(r.year, r.month)] = r.avg_rate || 0;
    });
    const data = labels.map((label) => map[label] ?? null);
    return {
      label: county,
      data,
      spanGaps: true,
    };
  });

  new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets,
    },
    options: {
      responsive: true,
      scales: { y: { beginAtZero: false } },
    },
  });
}

// 10. Role × sector matrix (clustered bar)
function buildRoleSectorMatrixChart() {
  const ctx = getCanvas("roleSectorMatrixChart");
  if (!ctx || !stats.role_sector_matrix) return;

  const rows = stats.role_sector_matrix;
  if (!rows.length) return;

  const sectors = [...new Set(rows.map((r) => r.sector))].slice(0, 6);
  const roles = [...new Set(rows.map((r) => r.role))].slice(0, 8);

  const datasets = roles.map((role) => {
    const data = sectors.map((sector) => {
      const match = rows.find(
        (r) => r.sector === sector && r.role === role,
      );
      return match && match.avg_rate ? match.avg_rate : 0;
    });
    return {
      label: role,
      data,
    };
  });

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: sectors,
      datasets,
    },
    options: {
      responsive: true,
      scales: {
        y: { beginAtZero: true },
      },
    },
  });
}

// Utility: download chart PNG -----------------------------------------
function downloadChart(id) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const link = document.createElement("a");
  link.href = canvas.toDataURL("image/png");
  link.download = `${id}.png`;
  link.click();
}

// Initialise everything once DOM is ready -----------------------------
document.addEventListener("DOMContentLoaded", () => {
  try {
    buildSectorChart();
    buildRoleChart();
    buildCountyPieChart();
    buildSectorPieChart();

    buildPayHistogram();
    buildSectorRangeChart();
    buildPayTrendChart();
    buildSectorVolatilityChart();
    buildSectorCountyHeatChart();
    buildTopCompaniesChart();
    buildOutlierScatterChart();
    buildRoleMixChart();
    buildCountyTrendChart();
    buildRoleSectorMatrixChart();
  } catch (e) {
    console.error("Error building insights charts:", e);
  }
});
