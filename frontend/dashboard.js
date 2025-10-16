// Wrap everything in an IIFE to avoid global namespace pollution
(function () {
  // API base URL - point to Flask backend
  const API_BASE_URL = "http://127.0.0.1:8000";

  // Chart configurations
  const chartConfigs = {
    importanceChart: null,
    priceHistoryChart: null,
  };

  // Create feature importance chart
  function createImportanceChart() {
    const ctx = document.getElementById("importanceChart").getContext("2d");
    chartConfigs.importanceChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: [
          "Price",
          "Sentiment",
          "USD/INR",
          "Repo",
          "Unemp",
          "NextEv",
          "SinceEv",
          "Window",
          "Impact",
        ],
        datasets: [
          {
            label: "Feature Importance",
            data: [],
            backgroundColor: "rgba(75, 192, 192, 0.6)",
            borderColor: "rgba(75, 192, 192, 1)",
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
            text: "Feature Importance Analysis",
            font: { size: 16, weight: "bold" },
          },
          legend: { position: "bottom" },
        },
        scales: {
          y: {
            beginAtZero: true,
            max: 0.5,
            title: { display: true, text: "Importance Score" },
          },
          x: {
            title: { display: true, text: "Features" },
          },
        },
        animation: { duration: 1000, easing: "easeInOutQuart" },
      },
    });
  }

  // Function to load feature importances from API
  async function loadFeatureImportances(ticker = null) {
    try {
      const endpoint = ticker
        ? `${API_BASE_URL}/feature_importances/${ticker}`
        : `${API_BASE_URL}/feature_importances`;
      const res = await fetch(endpoint);
      const data = await res.json();

      if (data.error) {
        console.error("Error loading importances:", data.error);
        return;
      }

      chartConfigs.importanceChart.data.datasets[0].data = data.importances;
      chartConfigs.importanceChart.update("active");
    } catch (error) {
      console.error("Failed to load feature importances:", error);
    }
  }

  // Function to get prediction
  async function getPrediction() {
    let ticker = document.getElementById("ticker").value;
    if (!ticker) {
      alert("Please enter a ticker symbol");
      return;
    }

    ticker = ticker.replace(".NS", "").toUpperCase();

    try {
      const res = await fetch(`${API_BASE_URL}/predict/${ticker}`);
      const data = await res.json();

      if (data.error) {
        document.getElementById("result").innerHTML = `
                    <div class="alert alert-danger">
                        <h5 class="alert-heading">Error</h5>
                        <p>${data.error}</p>
                    </div>`;
        return;
      }

      const isUp = data.prediction == 1;
      const color = isUp ? "success" : "danger";
      const directionSymbol = isUp ? "△" : "▽";
      const directionText = isUp ? "UP" : "DOWN";

      document.getElementById("result").innerHTML = `
                <div class="alert alert-${color} prediction-card">
                    <div class="d-flex justify-content-between align-items-center">
                        <h3 class="mb-0">${ticker}</h3>
                        <div class="prediction-badge badge bg-${color} p-2">
                            <span class="direction-symbol" style="font-size: 1.2em;">${directionSymbol}</span>
                            <span class="ms-1">${directionText}</span>
                        </div>
                    </div>
                    <div class="mt-3">
                        <label class="form-label">Confidence Level</label>
                        <div class="progress" style="height: 1.5rem;">
                            <div class="progress-bar bg-${color}" role="progressbar" 
                                 style="width: ${(
                                   data.confidence * 100
                                 ).toFixed(1)}%" 
                                 aria-valuenow="${(
                                   data.confidence * 100
                                 ).toFixed(1)}" 
                                 aria-valuemin="0" 
                                 aria-valuemax="100">
                                 ${(data.confidence * 100).toFixed(1)}%
                            </div>
                        </div>
                    </div>
                </div>`;

      await loadFeatureImportances(ticker);
    } catch (error) {
      document.getElementById("result").innerHTML = `
                <div class="alert alert-danger">Failed to fetch prediction: ${error.message}</div>`;
    }
  }

  // Initialize everything when the page loads
  document.addEventListener("DOMContentLoaded", () => {
    createImportanceChart();
    loadFeatureImportances();

    // Add click handler for predict button
    document
      .getElementById("predictBtn")
      .addEventListener("click", getPrediction);

    // Add click handlers for stock badges
    document.querySelectorAll(".stock-badge").forEach((badge) => {
      badge.addEventListener("click", () => {
        document.getElementById("ticker").value = badge.textContent;
        getPrediction();
      });
    });
  });

  // Make getPrediction available globally for inline button clicks
  window.getPrediction = getPrediction;
})();
