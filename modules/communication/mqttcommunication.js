/**
 * Responsible to handle software-level MQTT communication
 * It handles all communication with the supervisor and performs cooperation with other Worker instances
 */

var UUID = require("uuid");

var MQTT = require("../egsm-common/communication/mqttconnector")
var LOG = require('../egsm-common/auxiliary/logManager')
var AUX = require('../egsm-common/auxiliary/auxiliary')
var ROUTES = require('../communication/routes')

var EGSM_ENGINE = require('../egsmengine/egsmengine');
var EVENT_ROUTER = require('../eventrouter/eventrouter')

module.id = "MQTTCOMM"

const ID_VERIFICATION_PERIOD = 1500 //Time the other Workers has to reply if their ID is identical with the one the local worker wants to use

//Topic definitions
const WORKERS_TO_SUPERVISOR = 'workers_to_supervisor'
const SUPERVISOR_TO_WORKERS = 'supervisor_to_workers'
const WORKERS_TO_AGGREGATORS = 'workers_to_aggregators'
const AGGREGATORS_TO_WORKERS = 'aggregators_to_workers'
var TOPIC_SELF = ''

var MQTT_HOST = undefined
var MQTT_PORT = undefined;
var MQTT_USER = undefined;
var MQTT_USER_PW = undefined

var REQUEST_PROMISES = new Map()

/*Message body contains:
-sender_type: WORKER/AGGREGATOR
-sender_id: <string>
-message_type: PONG/PING/NEW_WORKER/NEW_ENGINE_SLOT/NEW_ENGINE_SLOT_RESP/NEW_ENGINE/SEARCH
-request_id: <string> (optional if no response expected)
-payload: <string>
*/

/**
 * MQTT Message handler of the module. Handles all messages from the Supervisor and other Workers
 * Please not that this function is not responsible for "process-related" messages, those are handled by the eventrouter.js
 * @param {string} hostname 
 * @param {int} port 
 * @param {string} topic 
 * @param {string} message 
 * @returns -
 */
async function onMessageReceived(hostname, port, topic, message) {
    LOG.logWorker('DEBUG', `New message received from topic: ${topic}`, module.id)
    if ((hostname != MQTT_HOST || port != MQTT_PORT) || (topic != SUPERVISOR_TO_WORKERS && topic != TOPIC_SELF && topic != AGGREGATORS_TO_WORKERS)) {
        LOG.logWorker('DEBUG', `Reveived message is not intended to handle here`, module.id)
        return
    }
    try {
        var msgJson = JSON.parse(message.toString())
    } catch (e) {
        LOG.logWorker('ERROR', `Error while parsing mqtt message: ${message}`, module.id)
        return
    }
    //The message has been published by the supervisor to the shared SUPERVISOR_TO_WORKERS
    //These messages have been delived to all other workers too
    if (topic == SUPERVISOR_TO_WORKERS) {
        switch (msgJson['message_type']) {
            case 'NEW_ENGINE_SLOT':
                LOG.logWorker('DEBUG', `NEW_ENGINE_SLOT requested`, module.id)
                if (EGSM_ENGINE.hasFreeSlot()) {
                    var response = {
                        request_id: msgJson['request_id'],
                        free_slots: EGSM_ENGINE.getCapacity() - EGSM_ENGINE.getEngineNumber(),
                        message_type: 'NEW_ENGINE_SLOT_RESP',
                        sender_id: TOPIC_SELF
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                }
                break;
            case 'PING':
                LOG.logWorker('DEBUG', `PING requested`, module.id)
                var response = {
                    request_id: msgJson['request_id'],
                    message_type: 'PONG',
                    sender_id: TOPIC_SELF,
                    payload: {
                        hostname: ROUTES.getRESTCredentials()['hostname'],
                        port: ROUTES.getRESTCredentials()['port'],
                        uptime: process.uptime(),
                        capacity: EGSM_ENGINE.getCapacity(),
                        engine_mumber: EGSM_ENGINE.getEngineNumber()
                    }
                }
                MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                break;
            case 'SEARCH':
                LOG.logWorker('DEBUG', `SEARCH requested for ${msgJson['payload']['engine_id']}`, module.id)
                if (EGSM_ENGINE.exists(msgJson['payload']['engine_id'])) {
                    var response = {
                        request_id: msgJson['request_id'],
                        message_type: 'SEARCH',
                        sender_id: TOPIC_SELF,
                        payload: { rest_api: ROUTES.getRESTCredentials() }
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                }
                break;
            case 'GET_COMPLETE_DIAGRAM':
                LOG.logWorker('DEBUG', `GET_COMPLETE_DIAGRAM requested for ${msgJson['payload']['engine_id']}`, module.id)
                if (EGSM_ENGINE.exists(msgJson['payload']['engine_id'])) {
                    var response = {
                        request_id: msgJson['request_id'],
                        message_type: 'GET_COMPLETE_DIAGRAM_RESP',
                        sender_id: TOPIC_SELF,
                        payload: { result: EGSM_ENGINE.getCompleteDiagram(msgJson['payload']['engine_id']) }
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                }
                break;
            case 'GET_COMPLETE_NODE_DIAGARM':
                LOG.logWorker('DEBUG', `GET_COMPLETE_NODE_DIAGARM requested for ${msgJson['payload']['engine_id']}`, module.id)
                if (EGSM_ENGINE.exists(msgJson['payload']['engine_id'])) {
                    var response = {
                        request_id: msgJson['request_id'],
                        message_type: 'GET_COMPLETE_NODE_DIAGARM_RESP',
                        sender_id: TOPIC_SELF,
                        payload: { result: EGSM_ENGINE.getCompleteNodeDiagram(msgJson['payload']['engine_id']) }
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                }
                break;
            case 'PROCESS_SEARCH':
                LOG.logWorker('DEBUG', `PROCESS_SEARCH requested for ${msgJson['payload']['process_id']}`, module.id)
                var engines = EGSM_ENGINE.getEnginesOfProcess(msgJson['payload']['process_id'])
                var api = ROUTES.getRESTCredentials()
                engines.forEach(element => {
                    element['worker_host'] = api.hostname
                    element['worker_api_port'] = api.port
                });
                var response = {
                    request_id: msgJson['request_id'],
                    message_type: 'PROCESS_SEARCH_RESP',
                    sender_id: TOPIC_SELF,
                    payload: { engines: engines }
                }
                MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                break;
            case 'DELETE_ENGINE':
                LOG.logWorker('DEBUG', `DELETE_ENGINE requested for ${msgJson['payload']['engine_id']}`, module.id)
                if (EGSM_ENGINE.exists(msgJson['payload']['engine_id'])) {
                    var response = {
                        request_id: msgJson['request_id'],
                        message_type: 'DELETE_ENGINE_RESP',
                        sender_id: TOPIC_SELF,
                        payload: { result: EGSM_ENGINE.removeEngine(msgJson['payload']['engine_id']) }
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                }
                break;
        }
    }

    else if (topic == AGGREGATORS_TO_WORKERS) {
        switch (msgJson['message_type']) {
            case 'PROCESS_GROUP_MEMBER_DISCOVERY':
                LOG.logWorker('DEBUG', `PROCESS_GROUP_MEMBER_DISCOVERY requested`, module.id)
                var result = EGSM_ENGINE.getFilteredProcesses(msgJson['payload']['rules'])
                if (result.length > 0) {
                    var response = {
                        request_id: msgJson['request_id'],
                        message_type: 'PROCESS_GROUP_MEMBER_DISCOVERY_RESP',
                        sender_id: TOPIC_SELF,
                        payload: { engines: result }
                    }
                    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_AGGREGATORS, JSON.stringify(response))
                }
        }
    }

    //These messages were sent by the Supervisor or by another Workers and only this Worker is receiveing it
    else if (topic == TOPIC_SELF) {
        switch (msgJson['message_type']) {
            case 'PONG':
                LOG.logWorker('DEBUG', `PONG received`, module.id)
                if (REQUEST_PROMISES.has(msgJson['request_id'])) {
                    REQUEST_PROMISES.get(msgJson['request_id'])('not_ok')
                    REQUEST_PROMISES.delete(msgJson['request_id'])
                }
                break;
            case 'PING':
                LOG.logWorker('DEBUG', `PING requested`, module.id)
                var response = {
                    request_id: msgJson['request_id'],
                    message_type: 'PONG'
                }
                MQTT.publishTopic(MQTT_HOST, MQTT_PORT, TOPIC_SELF, JSON.stringify(response))
                break;
            case 'NEW_ENGINE':
                LOG.logWorker('DEBUG', `NEW_ENGINE requested`, module.id)
                var resPayload = await createNewEngine(msgJson['payload'])
                var response = {
                    request_id: msgJson['request_id'],
                    payload: resPayload,
                    message_type: 'NEW_ENGINE'
                }
                MQTT.publishTopic(MQTT_HOST, MQTT_PORT, TOPIC_SELF, JSON.stringify(response))
                break;
            case 'GET_ENGINE_LIST':
                LOG.logWorker('DEBUG', `GET_ENGINE_LIST requested`, module.id)
                var resPayload = EGSM_ENGINE.getEngineList()
                var api = ROUTES.getRESTCredentials()
                resPayload.forEach(element => {
                    element['worker_host'] = api.hostname
                    element['worker_api_port'] = api.port
                });

                var response = {
                    request_id: msgJson['request_id'],
                    payload: resPayload,
                    message_type: 'GET_ENGINE_LIST_RESP'
                }
                MQTT.publishTopic(MQTT_HOST, MQTT_PORT, WORKERS_TO_SUPERVISOR, JSON.stringify(response))
                break;
        }
    }
}

/**
 * Creates a new eGSM engine in the EGSM_ENGINE module
 * @param {Object} payload The received payload in the request which contains all information and files which are necessary to create a new engine instance
 * @returns SUCCESS or ERROR objects with the appropriate message
 */
async function createNewEngine(payload) {
    LOG.logWorker('DEBUG', 'New engine creation requested', module.id)
    var responsePayload = {}

    //Check once again if the worker can serve one more engine
    if (!EGSM_ENGINE.hasFreeSlot()) {
        LOG.logWorker('WARNING', 'Request cancelled. Out of free Engine slots', module.id)
        responsePayload['error'] = 'NO_SLOT'
        return responsePayload
    }

    //Check if necessary data fields are available
    var engine_id = payload.engine_id
    var stakeholders = payload.stakeholders

    var mqtt_broker = payload.mqtt_broker
    var mqtt_port = payload.mqtt_port
    var mqtt_user = payload.mqtt_user
    var mqtt_password = payload.mqtt_password

    if (typeof engine_id == "undefined" || typeof stakeholders == "undefined" || typeof mqtt_broker == "undefined" || mqtt_broker == "undefined" || typeof mqtt_port == "undefined"
        || typeof mqtt_user == "undefined" || typeof mqtt_password == "undefined") {
        LOG.logWorker('WARNING', 'Engine creation request cancelled. Argument(s) are missing', module.id)
        responsePayload['error'] = 'ARGUMENT_MISSING'
        return responsePayload
    }

    //Check if each necessary files are available
    var informalModel = payload.informal_model;
    var processModel = payload.process_model;
    var eventRouterConfig = payload.event_router_config;

    if (typeof informalModel == 'undefined' || typeof processModel == 'undefined' || typeof eventRouterConfig == 'undefined') {
        LOG.logWorker('WARNING', 'Engine creation request cancelled. Necessary files have not received, process cannot be initiated!', module.id)
        responsePayload['error'] = 'FILE_MISSING'
        return responsePayload
    }

    //Everything is provided, creating new engine

    //Check if there is any engine with the same engine id
    if (EGSM_ENGINE.exists(engine_id)) {
        LOG.logWorker('WARNING', `Engine with id [${engine_id}] is already exists, could not created again`, module.id)
        responsePayload['error'] = 'ENGINE_ID_CONFLICT'
        return responsePayload
    }
    //Check if the broker connection exists and create it is if not
    var result = MQTT.createConnection(mqtt_broker, mqtt_port, mqtt_user, mqtt_password, 'ntstmqtt_' + Math.random().toString(16).substr(2, 8));
    if (!result) {
        LOG.logWorker('WARNING', 'Error while creating connection', module.id)
        responsePayload['error'] = 'BROKER_CONN_ERROR'
        return responsePayload
    }
    if (result == 'created') {
        LOG.logWorker('DEBUG', 'New broker connection created for the new engine', module.id)
    }
    else if (result == 'connection_exists') {
        LOG.logWorker('DEBUG', 'New Broker connection was not needed, since it was already defined', module.id)
    }

    //Set up Default Broker for the engine
    EVENT_ROUTER.setEngineDefaults(engine_id, mqtt_broker, mqtt_port, EGSM_ENGINE.updateInfoModel)

    //Adding Engine to the Event Router
    EVENT_ROUTER.initConnections(engine_id, eventRouterConfig)

    //Creating new engine - PROPERLY AWAIT THIS
    try {
        var value = await EGSM_ENGINE.createNewEngine(engine_id, informalModel, processModel, stakeholders)
        if (value == 'created') {
            LOG.logWorker('DEBUG', 'New engine created', module.id)
            responsePayload['success'] = 'ENGINE_CREATED'
        } else {
            LOG.logWorker('WARNING', 'Unhandled error while creating new engine', module.id)
            responsePayload['error'] = 'ERROR_WHILE_CREATING_ENGINE'
        }
    } catch (error) {
        LOG.logWorker('ERROR', `Error creating engine: ${error}`, module.id)
        responsePayload['error'] = 'ERROR_WHILE_CREATING_ENGINE'
    }
    
    return responsePayload
}

/**
 * Wrapper function to execute delay
 * @param {int} delay Required dely in millis
 */
async function wait(delay) {
    await AUX.sleep(delay)
}

/**
 * Executes an ID uniqueness verification through cooperating with other Workers 
 * @param {string} candidate ID candidate to verify uniqueness
 * @returns A Promise, which becomes 'ok' if the ID was unique 'not_ok' otherwise
 */
async function checkIdCandidate(candidate) {
    var request_id = UUID.v4();
    var message = JSON.stringify(
        request_id = request_id,
        message_type = 'PING'
    )
    MQTT.publishTopic(MQTT_HOST, MQTT_PORT, candidate, JSON.stringify(message))
    var promise = new Promise(function (resolve, reject) {
        REQUEST_PROMISES.set(request_id, resolve)

        wait(ID_VERIFICATION_PERIOD).then(() => {
            resolve('ok')
        })
    });
    return promise
}

/**
 * Init Broker connection the module will use. It will also find a unique ID for the Worker itself
 * @param {Broker} broker Broker credentials
 * @returns Returns the own ID of the Worker
 */
async function initPrimaryBrokerConnection(broker) {
    MQTT_HOST = broker.host
    MQTT_PORT = broker.port
    MQTT_USER = broker.username
    MQTT_USER_PW = broker.password

    MQTT.init(onMessageReceived)
    MQTT.createConnection(MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_USER_PW)

    //Find an unused, unique ID for the Engine
    while (true) {
        TOPIC_SELF = UUID.v4();
        await MQTT.subscribeTopic(MQTT_HOST, MQTT_PORT, TOPIC_SELF)
        var result = await checkIdCandidate(TOPIC_SELF)
        if (result == 'ok') {
            break;
        }
        else {
            MQTT.unsubscribeTopic(MQTT_HOST, MQTT_PORT, TOPIC_SELF)
        }
    }
    await MQTT.subscribeTopic(MQTT_HOST, MQTT_PORT, SUPERVISOR_TO_WORKERS)
    await MQTT.subscribeTopic(MQTT_HOST, MQTT_PORT, AGGREGATORS_TO_WORKERS)
    return TOPIC_SELF
}

module.exports = {
    checkIdCandidate: checkIdCandidate,
    initPrimaryBrokerConnection: initPrimaryBrokerConnection
}