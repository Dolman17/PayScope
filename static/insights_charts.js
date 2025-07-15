// Grouping helpers
function groupBy(records, key) {
  return records.reduce((acc, record) => {
    const val = record[key] || "Unknown";
    acc[val] = acc[val] || [];
    acc[val].push(record);
    return acc;
  }, {});
}

function average(arr, key) {
  const total = arr.reduce((sum, obj) => sum + (obj[key] || 0), 0);
  return arr.length ? total / arr.length : 0;
}

function countBy(records, key) {
  return records.reduce((acc, record) => {
    const val = record[key] || "Unknown";
    acc[val] = (acc[val] || 0) + 1;
    return acc;
  }, {});
}

// Chart rendering functions
function renderBarChart(ctxId, labels, values, labelText) {
  const ctx = document.getElementById(ctxId);
  if (!ctx) return;
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: labelText,
        data: values,
        backgroundColor: '#0160B1'
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: {
          label: context => `£${context.parsed.y.toFixed(2)}`
        }}
      },
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: "£ per hour" }
        }
      }
    }
  });
}

function renderPieChart(ctxId, dataObj, labelText) {
  const ctx = document.getElementById(ctxId);
  if (!ctx) return;

  const labels = Object.keys(dataObj);
  const values = Object.values(dataObj);

  new Chart(ctx, {
    type: 'pie',
    data: {
      labels,
      datasets: [{
        label: labelText,
        data: values,
        backgroundColor: labels.map((_, i) =>
          `hsl(${(i * 137.5) % 360}, 70%, 60%)`)
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: {
          position: 'right',
          labels: { boxWidth: 12 }
        }
      }
    }
  });
}

// Data prep
const sectorGroups = groupBy(records, "sector");
const roleGroups = groupBy(records, "job_role");

const avgPayBySector = Object.fromEntries(
  Object.entries(sectorGroups).map(([k, v]) => [k, average(v, "pay_rate")])
);

const avgPayByRole = Object.fromEntries(
  Object.entries(roleGroups).map(([k, v]) => [k, average(v, "pay_rate")])
);

const countByCounty = countBy(records, "county");
const countBySector = countBy(records, "sector");

// Draw charts
renderBarChart("sectorChart", Object.keys(avgPayBySector), Object.values(avgPayBySector), "Avg Pay by Sector");
renderBarChart("roleChart", Object.keys(avgPayByRole), Object.values(avgPayByRole), "Avg Pay by Role");
renderPieChart("countyPieChart", countByCounty, "Records by County");
renderPieChart("sectorPieChart", countBySector, "Records by Sector");


function downloadChart(canvasId) {
  const canvas = document.getElementById(canvasId);
  const link = document.createElement('a');
  link.download = `${canvasId}.png`;
  link.href = canvas.toDataURL('image/png', 1.0);
  link.click();
}
