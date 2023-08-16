const express = require('express'),
  router = express.Router(),
  telemetryService = require('../service/telemetry-service');

router.post('/v1/telemetry', (req, res) => telemetryService.dispatch(req, res));

router.get('/health', (req, res) => telemetryService.health(req, res));

router.get('/v1/getData', (req, res) => telemetryService.getData(req, res));

router.get('/v1/getCount', (req, res) => {
  console.log("query", req.query.event)
  telemetryService.getCount(req, res)
});

module.exports = router;
