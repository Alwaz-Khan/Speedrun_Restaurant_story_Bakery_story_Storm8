let data = [];

const baseTable = [
  1,5,10,15,30,45,60,120,180,360,540,720,1440,2160,2880
];

fetch("./master_table.json")
  .then(res => res.json())
  .then(json => {
    data = json;
    render();
  });


fetch("./master_table.json")
  .then(res => res.json())
  .then(json => {
    data = json;
    console.log("Data loaded:", data.length, "rows");
    console.log("Sample row:", data[0]);
    render();
  })
  .catch(err => {
    console.error("Failed to load JSON:", err);
  });

function checkDifficulty(filter, value) {
  if (filter === "easy") return value === 2;
  if (filter === "medium") return value <= 3;
  if (filter === "hard") return value <= 6;
  return false;
}

function formatTime(minutes) {
  if (minutes < 60) return `${minutes} min`;
  const hours = minutes / 60;
  return `${hours % 1 === 0 ? hours : hours.toFixed(1)} hr`;
}

function getBest(filters, column) {
  const filtered = data.filter(row =>
    row.rcp_time_min <= filters.return_time &&
    row.rcp_level <= filters.level &&
    row.game_mode === filters.mode &&
    checkDifficulty(filters.difficulty, row.rcp_difficulty)
  );

  if (filtered.length === 0) return null;

  return filtered.reduce((best, row) =>
    row[column] > (best?.[column] ?? -Infinity) ? row : best
  , null);
}

function render() {
  const filters = {
    mode: document.getElementById("mode").value,
    level: Number(document.getElementById("level").value),
    difficulty: document.getElementById("difficulty").value
  };

  const tbody = document.getElementById("table-body");
  tbody.innerHTML = "";

  baseTable.forEach(return_time => {
    const xpBest = getBest({ ...filters, return_time }, "rcp_xp");
    const profitBest = getBest({ ...filters, return_time }, "rcp_profit");

    const rowHTML = `
      <tr>
        <td>${formatTime(return_time)}</td>

        <td>${xpBest?.rcp_name || "-"}</td>
        <td>${xpBest?.appl_name || "-"}</td>
        <td>${xpBest?.rcp_xp || "-"}</td>
        <td>${xpBest ? Math.round((xpBest.rcp_xp * 60) / return_time) : "-"}</td>

        <td>${profitBest?.rcp_name || "-"}</td>
        <td>${profitBest?.appl_name || "-"}</td>
        <td>${profitBest?.rcp_profit || "-"}</td>
        <td>${profitBest ? Math.round((profitBest.rcp_profit * 60) / return_time) : "-"}</td>
      </tr>
    `;

    tbody.innerHTML += rowHTML;
  });
}