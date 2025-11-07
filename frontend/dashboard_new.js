// Add to the existing chartConfigs object
chartConfigs.technicalChart = null;

// Function to load technical indicators
async function loadTechnicalIndicators(ticker) {
  try {
    const res = await fetch(`${API_BASE_URL}/technical/${ticker}`);
    if (!res.ok) {
      console.warn("Technical indicators not available:", res.status);
      return;
    }
    const data = await res.json();

    // Update RSI
    document.getElementById("rsiValue").textContent = data.rsi;
    document.getElementById("rsiInterpretation").textContent =
      data.rsi_interpretation;
    document.getElementById("rsiTrend").innerHTML =
      data.rsi > 50
        ? '<i class="bi bi-arrow-up-circle-fill text-success"></i>'
        : '<i class="bi bi-arrow-down-circle-fill text-danger"></i>';

    // Update MACD
    document.getElementById("macdValue").textContent = data.macd;
    document.getElementById("macdInterpretation").textContent =
      data.macd_interpretation;
    document.getElementById("macdTrend").innerHTML =
      data.macd_interpretation === "Bullish"
        ? '<i class="bi bi-arrow-up-circle-fill text-success"></i>'
        : '<i class="bi bi-arrow-down-circle-fill text-danger"></i>';
  } catch (error) {
    console.error("Failed to load technical indicators:", error);
  }
}

// Function to load price statistics
async function loadPriceStatistics(ticker) {
  try {
    const res = await fetch(`${API_BASE_URL}/statistics/${ticker}`);
    if (!res.ok) {
      console.warn("Price statistics not available:", res.status);
      return;
    }
    const data = await res.json();

    // Update price statistics
    document.getElementById(
      "week52High"
    ).textContent = `₹${data.week_52_high.toLocaleString()}`;
    document.getElementById(
      "week52Low"
    ).textContent = `₹${data.week_52_low.toLocaleString()}`;
    document.getElementById("avgVolume").textContent =
      data.avg_volume.toLocaleString();

    const changeEl = document.getElementById("dailyChange");
    const changeValue = data.daily_change_percent;
    changeEl.textContent = `${changeValue > 0 ? "+" : ""}${changeValue}%`;
    changeEl.className = `h5 mb-0 ${
      changeValue > 0 ? "text-success" : "text-danger"
    }`;
  } catch (error) {
    console.error("Failed to load price statistics:", error);
  }
}

// Watchlist functions
async function loadWatchlist() {
  try {
    const res = await fetch(`${API_BASE_URL}/watchlist`);
    if (!res.ok) return;
    const watchlist = await res.json();

    const container = document.getElementById("watchlistContainer");
    container.innerHTML = watchlist.length
      ? ""
      : '<div class="col-12">No stocks in watchlist</div>';

    watchlist.forEach((item) => {
      const card = document.createElement("div");
      card.className = "col-md-4 mb-3";
      card.innerHTML = `
        <div class="card">
          <div class="card-body">
            <div class="d-flex justify-content-between align-items-center">
              <h5 class="mb-0">${item.ticker}</h5>
              <button class="btn btn-sm btn-outline-danger remove-watchlist" data-ticker="${
                item.ticker
              }">
                <i class="bi bi-trash"></i>
              </button>
            </div>
            <div class="text-muted small">Added ${new Date(
              item.added_at
            ).toLocaleDateString()}</div>
          </div>
        </div>
      `;
      container.appendChild(card);
    });

    // Add click handlers for remove buttons
    document.querySelectorAll(".remove-watchlist").forEach((btn) => {
      btn.addEventListener("click", async (e) => {
        const ticker = e.currentTarget.dataset.ticker;
        await removeFromWatchlist(ticker);
      });
    });
  } catch (error) {
    console.error("Failed to load watchlist:", error);
  }
}

async function addToWatchlist(ticker) {
  try {
    const res = await fetch(`${API_BASE_URL}/watchlist`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker }),
    });
    if (!res.ok) throw new Error("Failed to add to watchlist");
    await loadWatchlist();
  } catch (error) {
    console.error("Failed to add to watchlist:", error);
  }
}

async function removeFromWatchlist(ticker) {
  try {
    const res = await fetch(`${API_BASE_URL}/watchlist`, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker }),
    });
    if (!res.ok) throw new Error("Failed to remove from watchlist");
    await loadWatchlist();
  } catch (error) {
    console.error("Failed to remove from watchlist:", error);
  }
}

// Update the existing getPrediction function
const originalGetPrediction = getPrediction;
getPrediction = async function () {
  await originalGetPrediction();
  const ticker = document.getElementById("ticker").value;
  if (!ticker) return;

  // Load additional data
  await Promise.all([
    loadTechnicalIndicators(ticker),
    loadPriceStatistics(ticker),
  ]);
};

// Initialize everything when the page loads
document.addEventListener("DOMContentLoaded", () => {
  // ... (existing initialization code) ...

  // Add watchlist button handler
  const addWatchlistBtn = document.getElementById("addToWatchlist");
  if (addWatchlistBtn) {
    addWatchlistBtn.addEventListener("click", () => {
      const ticker = document.getElementById("ticker").value;
      if (ticker) addToWatchlist(ticker);
    });
  }

  // Load initial watchlist
  loadWatchlist();
});
