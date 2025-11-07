// Wrap everything in an IIFE to avoid global namespace pollution
(function () {
  // Setup navigation when DOM is loaded
  // Global state to track current view
  let currentView = "main"; // Can be 'main' or 'about'

  // Function to stop ongoing data fetching
  function stopDataFetching() {
    // Clear any existing intervals
    if (window.dataFetchingInterval) {
      clearInterval(window.dataFetchingInterval);
    }
  }

  // Function to show main view
  function showMainView() {
    currentView = "main";
    document.querySelector(".search-container").style.display = "block";
    document.querySelector("#recent").style.display = "block";
    document.querySelector("#about").style.display = "none";

    // Resume any data fetching if needed
    loadWatchlist(); // Only load if we're in main view
  }

  // Function to show about view
  function showAboutView() {
    currentView = "about";
    document.querySelector(".search-container").style.display = "none";
    document.querySelector("#recent").style.display = "none";
    document.querySelector("#about").style.display = "block";

    // Stop any ongoing data fetching
    stopDataFetching();
  }

  document.addEventListener("DOMContentLoaded", function () {
    // Handle navigation
    document.querySelectorAll(".nav-link").forEach((link) => {
      link.addEventListener("click", function (e) {
        e.preventDefault();
        const href = this.getAttribute("href");

        if (href === "#about") {
          showAboutView();
        } else if (href === "#recent" || href === "#") {
          showMainView();
        }
      });
    });
  });

  // API base URL - point to Flask backend
  const API_BASE_URL = "http://127.0.0.1:8000";

  // Chart configurations
  const chartConfigs = {
    importanceChart: null,
    priceHistoryChart: null,
    technicalChart: null,
  };

  // UI helpers
  function showLoading() {
    const loader = document.getElementById("loaderPlaceholder");
    if (loader) loader.style.display = "inline-block";
    const btn = document.getElementById("predictBtn");
    if (btn) btn.setAttribute("disabled", "disabled");
  }

  function hideLoading() {
    const loader = document.getElementById("loaderPlaceholder");
    if (loader) loader.style.display = "none";
    const btn = document.getElementById("predictBtn");
    if (btn) btn.removeAttribute("disabled");
  }

  // Create feature importance chart
  function createImportanceChart() {
    const ctx = document.getElementById("importanceChart").getContext("2d");
    chartConfigs.importanceChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: [],
        datasets: [
          {
            label: "Feature Importance",
            data: [],
            backgroundColor: "rgba(54, 162, 235, 0.7)",
            borderColor: "rgba(54, 162, 235, 1)",
            borderWidth: 1,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: "Feature Importance",
            font: { size: 16, weight: "bold" },
          },
          legend: { display: false },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Score" } },
          x: { title: { display: true, text: "Feature" } },
        },
      },
    });
  }

  // Create price history chart
  function createPriceHistoryChart() {
    const ctx = document.getElementById("priceHistoryChart").getContext("2d");
    chartConfigs.priceHistoryChart = new Chart(ctx, {
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label: "Price",
            data: [],
            borderColor: "#198754",
            backgroundColor: "rgba(25,135,84,0.1)",
            tension: 0.2,
            pointRadius: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: { display: true, text: "Price History" },
          tooltip: {
            callbacks: {
              label: (context) => `₹${context.parsed.y.toLocaleString()}`,
            },
          },
        },
        scales: {
          x: {
            type: "time",
            time: {
              unit: "day",
              displayFormats: {
                day: "MMM d, yyyy",
              },
            },
            title: {
              display: true,
              text: "Date",
            },
          },
          y: {
            title: {
              display: true,
              text: "Price (₹)",
            },
            ticks: {
              callback: (value) => `₹${value.toLocaleString()}`,
            },
          },
        },
        interaction: {
          mode: "index",
          intersect: false,
        },
      },
    });
  }

  // Function to load feature importances from API (graceful fallback)
  async function loadFeatureImportances(ticker = null) {
    try {
      const endpoint = ticker
        ? `${API_BASE_URL}/feature_importances/${ticker}`
        : `${API_BASE_URL}/feature_importances`;
      const res = await fetch(endpoint);
      if (!res.ok) {
        console.warn(
          "Feature importances endpoint not available (",
          res.status,
          ")"
        );
        return;
      }
      const data = await res.json();

      if (data.error) {
        console.error("Error loading importances:", data.error);
        return;
      }

      // If server returns labels + importances, use them; otherwise fall back to known list
      const labels = data.labels || [
        "Price",
        "Sentiment",
        "USD/INR",
        "Repo",
        "Unemp",
        "NextEv",
        "SinceEv",
        "Window",
        "Impact",
      ];
      const importances = data.importances || data.scores || [];

      chartConfigs.importanceChart.data.labels = labels;
      chartConfigs.importanceChart.data.datasets[0].data = importances;
      chartConfigs.importanceChart.update("active");
    } catch (error) {
      console.error("Failed to load feature importances:", error);
    }
  }

  // Function to load price history (optional endpoint)
  async function loadPriceHistory(ticker) {
    try {
      const endpoint = `${API_BASE_URL}/price_history/${ticker}`;
      const res = await fetch(endpoint);
      if (!res.ok) {
        // endpoint missing or returns 404
        console.warn("Price history not available:", res.status);
        // Clear chart
        chartConfigs.priceHistoryChart.data.labels = [];
        chartConfigs.priceHistoryChart.data.datasets[0].data = [];
        chartConfigs.priceHistoryChart.update();
        return;
      }
      const data = await res.json();
      // Expect data: { dates: [...], prices: [...] } but tolerate alternatives
      let dates = data.dates || data.x || [];
      let prices = data.prices || data.y || data.values || [];

      // If backend returned objects like [{date:..., price:...}, ...]
      if (
        (!dates || dates.length === 0) &&
        Array.isArray(data) &&
        data.length &&
        data[0].date
      ) {
        dates = data.map((d) => d.date);
        prices = data.map((d) => d.price);
      }

      // normalize dates to JS Date or ISO strings acceptable by chart adapter
      const normalizedDates = dates.map((d) => {
        if (d === null || d === undefined) return null;
        // number: could be ms or seconds
        if (typeof d === "number") {
          // if looks like seconds (10 digits), convert to milliseconds
          return d < 1e12 ? new Date(d * 1000) : new Date(d);
        }
        // if already a Date instance
        if (d instanceof Date) return d;
        // try parse ISO or other formats - new Date should handle ISO
        const parsed = new Date(d);
        if (!isNaN(parsed)) return parsed;
        // fallback: try Date.parse or keep as-is (adapter may still accept)
        return new Date(Date.parse(String(d)));
      });

      chartConfigs.priceHistoryChart.data.labels = normalizedDates;
      chartConfigs.priceHistoryChart.data.datasets[0].data = prices;
      chartConfigs.priceHistoryChart.update();
    } catch (error) {
      console.error("Failed to load price history:", error);
    }
  }

  // Add entry to recent predictions table
  function addRecentPrediction({ ticker, direction, confidence }) {
    const tbody = document.querySelector("#recentTable tbody");
    if (!tbody) return;
    // remove placeholder row if present
    if (
      tbody.children.length === 1 &&
      tbody.children[0].textContent.includes("No predictions")
    ) {
      tbody.innerHTML = "";
    }
    const tr = document.createElement("tr");
    const now = new Date().toLocaleString();
    tr.innerHTML = `
      <td>${now}</td>
      <td><button class="btn btn-link p-0 recent-ticker">${ticker}</button></td>
      <td>${direction}</td>
      <td>${(confidence * 100).toFixed(1)}%</td>
    `;
    tbody.prepend(tr);

    // add click handler to re-run prediction when clicking ticker
    tr.querySelectorAll(".recent-ticker").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.getElementById("ticker").value = btn.textContent;
        getPrediction();
      });
    });
  }

  // Function to update timeframe predictions display
  function updateTimeframePredictions(predictions, accuracy) {
    const container = document.getElementById("timeframePredictions");
    if (!container) return;

    container.style.display = "block";

    Object.entries(predictions).forEach(([timeframe, data]) => {
      const el = container.querySelector(`[data-timeframe="${timeframe}"]`);
      if (!el) return;

      const isUp = data.prediction === 1;
      const directionText = isUp ? "UP" : "DOWN";
      const confidence = (data.confidence * 100).toFixed(1);

      // Update prediction
      el.querySelector(".h3").innerHTML = `
        <span class="text-${isUp ? "success" : "danger"}">
          ${directionText}
          <i class="bi bi-arrow-${isUp ? "up" : "down"}-circle-fill ms-2"></i>
        </span>
      `;

      // Update confidence bar
      const progressBar = el.querySelector(".progress-bar");
      progressBar.style.width = `${confidence}%`;
      progressBar.className = `progress-bar bg-${isUp ? "success" : "danger"}`;

      // Update accuracy stats if available
      const accuracyStats = accuracy[timeframe];
      if (accuracyStats) {
        const statsText = `
          ${accuracyStats.accuracy}% accuracy
          (${accuracyStats.verified_predictions}/${accuracyStats.total_predictions} verified)
        `;
        el.querySelector(".accuracy-stats").textContent = statsText;
      }
    });
  }

  // Function to get prediction
  async function getPrediction() {
    let ticker = document.getElementById("ticker").value;
    if (!ticker) {
      alert("Please enter a ticker symbol");
      return;
    }

    ticker = ticker.replace(".NS", "").toUpperCase();
    showLoading();

    try {
      // Get predictions for all timeframes
      const timeframesRes = await fetch(
        `${API_BASE_URL}/predict/timeframes/${ticker}`
      );
      if (!timeframesRes.ok) {
        throw new Error(
          `Failed to get timeframe predictions: ${timeframesRes.status}`
        );
      }
      const timeframeData = await timeframesRes.json();
      updateTimeframePredictions(
        timeframeData.predictions,
        timeframeData.accuracy
      );

      // Get main prediction
      const res = await fetch(`${API_BASE_URL}/predict/${ticker}`);
      if (!res.ok) {
        const text = await res.text();
        document.getElementById(
          "result"
        ).innerHTML = `<div class="alert alert-warning">Prediction endpoint returned ${res.status}: ${text}</div>`;
        hideLoading();
        return;
      }
      const data = await res.json();

      if (data.error) {
        document.getElementById("result").innerHTML = `
          <div class="alert alert-danger">
            <h5 class="alert-heading">Error</h5>
            <p>${data.error}</p>
          </div>`;
        hideLoading();
        return;
      }

      const isUp = data.prediction == 1;
      const color = isUp ? "success" : "danger";
      const directionSymbol = isUp ? "△" : "▽";
      const directionText = isUp ? "UP" : "DOWN";

      document.getElementById("result").innerHTML = `
        <div class="card prediction-card">
          <div class="card-body">
            <div class="d-flex justify-content-between align-items-start">
              <div>
                <h3 class="mb-0">${ticker}</h3>
                <div class="text-muted small">Model prediction</div>
              </div>
              <div class="text-end">
                <div class="badge bg-${color} direction-badge">${directionSymbol} ${directionText}</div>
              </div>
            </div>
            <div class="mt-3">
              <label class="form-label mb-1">Confidence Level</label>
              <div class="progress" style="height: 1.5rem;">
                <div class="progress-bar bg-${color}" role="progressbar"
                     style="width: ${(data.confidence * 100).toFixed(1)}%"
                     aria-valuenow="${(data.confidence * 100).toFixed(
                       1
                     )}" aria-valuemin="0" aria-valuemax="100">
                  ${(data.confidence * 100).toFixed(1)}%
                </div>
              </div>
              <div class="mt-2 small text-muted">Probability breakdown and feature influence shown to the right.</div>
            </div>
          </div>
        </div>`;

      // update charts (gracefully handle endpoints if missing)
      await Promise.all([
        loadFeatureImportances(ticker),
        loadPriceHistory(ticker),
      ]);

      // add to recent table
      addRecentPrediction({
        ticker,
        direction: directionText,
        confidence: data.confidence || 0,
      });
    } catch (error) {
      document.getElementById("result").innerHTML = `
        <div class="alert alert-danger">Failed to fetch prediction: ${error.message}</div>`;
      console.error(error);
    } finally {
      hideLoading();
    }
  }

  // Technical indicators functions
  async function loadTechnicalIndicators(ticker) {
    // Don't load technical indicators if we're in about view
    if (currentView === "about") return;

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

  // Price statistics functions
  async function loadPriceStatistics(ticker) {
    // Don't load price statistics if we're in about view
    if (currentView === "about") return;

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
    // Don't load watchlist if we're in about view
    if (currentView === "about") return;

    try {
      const res = await fetch(`${API_BASE_URL}/watchlist`);
      if (!res.ok) return;
      const watchlist = await res.json();

      const container = document.getElementById("watchlistContainer");
      // Check if container exists before trying to modify it
      if (!container) return;

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

  // Enhance the getPrediction function
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
    createImportanceChart();
    createPriceHistoryChart();
    loadFeatureImportances();

    // Add click handler for predict button
    const predictBtn = document.getElementById("predictBtn");
    if (predictBtn) predictBtn.addEventListener("click", getPrediction);

    // Add click handlers for stock badges
    document.querySelectorAll(".stock-badge").forEach((badge) => {
      badge.addEventListener("click", () => {
        document.getElementById("ticker").value = badge.textContent;
        getPrediction();
      });
    });

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

  // Make getPrediction available globally for inline button clicks
  window.getPrediction = getPrediction;
})();
