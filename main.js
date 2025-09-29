var fs = require('fs');

var PRIM = require('./modules/egsm-common/auxiliary/primitives')
var event_router = require('./modules/eventrouter/eventrouter')
var LOG = require('./modules/egsm-common/auxiliary/logManager');
var MQTTCOMM = require('./modules/communication/mqttcommunication')
var egsmengine = require('./modules/egsmengine/egsmengine');
var DBCONFIG = require('./modules/egsm-common/database/databaseconfig');
var CONNCONFIG = require('./modules/egsm-common/config/connectionconfig');

const CONFIG_FILE = './config.xml'

module.id = "MAIN"

var filecontent = fs.readFileSync(CONFIG_FILE, 'utf8')
CONNCONFIG.applyConfig(filecontent)

DBCONFIG.initDatabaseConnection(CONNCONFIG.getConfig().database_host, CONNCONFIG.getConfig().database_port, CONNCONFIG.getConfig().database_region,
    CONNCONFIG.getConfig().database_access_key_id, CONNCONFIG.getConfig().database_secret_access_key)

LOG.logWorker('DEBUG', 'Worker started', module.id)

var WORKER_ID = ''

var broker = new PRIM.Broker('localhost', 1883, '', '')

LOG.logWorker('DEBUG', 'Finding a unique ID by active cooperation with peers...', module.id)
MQTTCOMM.initPrimaryBrokerConnection(broker).then((result) => {
    WORKER_ID = result
    LOG.logWorker('DEBUG', `Unique ID found: [${WORKER_ID}]`, module.id)
})

egsmengine.setEventRouter(event_router.processPublish)