const express = require("express");

const app = express();
const PORT = process.env.PORT || 5001;

app.use(express.json());

app.get("/health", (req, res) => {
  res.json({ status: "ETL running" });
});

app.listen(PORT, () => {
  console.log(`ETL server running on port ${PORT}`);
});