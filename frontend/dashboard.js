async function getPrediction() {
  const ticker = document.getElementById('ticker').value;
  const res = await fetch(`/predict/${ticker}`);
  const data = await res.json();
  document.getElementById('result').innerHTML =
    `<h4>${ticker}: ${data.prediction==1?'UP':'DOWN'} (Conf: ${(data.confidence*100).toFixed(1)}%)</h4>`;
}

const ctx = document.getElementById('importanceChart').getContext('2d');
new Chart(ctx, {
  type: 'bar',
  data: {
    labels: ['Price','Sentiment','USD/INR','Repo','Unemp','NextEv','SinceEv','Window','Impact'],
    datasets: [{label:'Feature Importance',data:[0.30,0.10,0.15,0.14,0.10,0.05,0.05,0.06,0.05]}]
  }
});
