"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const https_1 = __importDefault(require("https"));
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const debug_1 = __importDefault(require("debug"));
const q_1 = __importDefault(require("q"));
const Table_1 = __importDefault(require("./Table"));
const Schema_1 = __importDefault(require("./Schema"));
const Model_1 = __importDefault(require("./Model"));
const VirtualType_1 = __importDefault(require("./VirtualType"));
const errors_1 = __importDefault(require("./errors"));
const debug = debug_1.default('dynamoose');
const debugTransaction = debug_1.default('dynamoose:transaction');
function createLocalDb(endpointURL) {
    const dynamoConfig = {};
    // This has to be done as the aws sdk types insist that new AWS.Endpoint(endpointURL) is not a string
    dynamoConfig['endpoint'] = new aws_sdk_1.default.Endpoint(endpointURL);
    return new aws_sdk_1.default.DynamoDB(dynamoConfig);
}
exports.createLocalDb = createLocalDb;
function getModelSchemaFromIndex(item, dynamoose) {
    const requestItem = item;
    const [requestItemProperty] = Object.keys(item);
    const tableName = requestItem[requestItemProperty].TableName;
    const TheModel = requestItem._dynamooseModel;
    if (!TheModel) {
        const errorMessage = `${tableName} is not a registered model. You can only use registered Dynamoose models when using a RAW transaction object.`;
        throw new errors_1.default.TransactionError(errorMessage);
    }
    const TheModel$ = TheModel.$__;
    const { schema } = TheModel$;
    return { TheModel, TheModel$, schema };
}
exports.getModelSchemaFromIndex = getModelSchemaFromIndex;
/**
 * @class Dynamoose
 * The main export of dyanmoose, this class houses all of the model, table, and config
 * functionality. All calls to any submodule occur through this class.
 */
class Dynamoose {
    /**
     * @constructor
     * This set's our default options, initializes our models object, and adds these methods:
     *    VirtualType
     *    AWS
     *    Schema
     *    Table
     *    Dynamoose
     *
     * These are the externally availbale modules.
     */
    constructor() {
        this.models = {};
        this.defaults = {
            'create': true,
            'waitForActive': true,
            'waitForActiveTimeout': 180000,
            'prefix': '',
            'suffix': '' // Table_suffix
        };
        this.VirtualType = VirtualType_1.default;
        this.AWS = aws_sdk_1.default;
        this.Schema = Schema_1.default;
        this.Table = Table_1.default;
        this.Dynamoose = this;
    }
    /**
     * This method adds a new model or returns the existing one if not unique.
     * @param name The chosen name for your model
     * @param schema The defined Schema
     * @param options The supported set of Dynamoose and Schema options
     * @returns an instance of your started Model
     */
    model(name, schema, options) {
        options = options || {};
        for (const key in this.defaults) {
            options[key] = typeof options[key] === 'undefined' ? this.defaults[key] : options[key];
        }
        name = options.prefix + name + options.suffix;
        debug('Looking up model %s', name);
        if (this.models[name]) {
            return this.models[name];
        }
        if (!(schema instanceof Schema_1.default)) {
            schema = new Schema_1.default(schema, options);
        }
        const model = Model_1.default.compile(name, schema, options, this);
        this.models[name] = model;
        return model;
    }
    /**
     * This methods sets up and attaches a local db instance
     * @param url the url to connect to locally
     */
    local(url) {
        this.endpointURL = url || 'http://localhost:8000';
        this.dynamoDB = createLocalDb(this.endpointURL);
        debug('Setting DynamoDB to local (%s)', this.endpointURL);
    }
    /**
     * This method will initialize and then return the dynamoDocumentClient
     * @returns an instance of the AWS.DynamoDB.DocumentClient
     */
    documentClient() {
        if (this.dynamoDocumentClient) {
            return this.dynamoDocumentClient;
        }
        if (this.endpointURL) {
            debug('Setting dynamodb document client to %s', this.endpointURL);
            this.AWS.config.update({ 'endpoint': this.endpointURL });
        }
        else {
            debug('Getting default dynamodb document client');
        }
        this.dynamoDocumentClient = new this.AWS.DynamoDB.DocumentClient();
        return this.dynamoDocumentClient;
    }
    /**
     * This method allows you to ovveride the built AWS.DynamoDB.DocumentClient instance
     * @param documentClient your AWS.DynamoDB.DocumentClient instance
     */
    setDocumentClient(documentClient) {
        debug('Setting dynamodb document client');
        this.dynamoDocumentClient = documentClient;
    }
    /**
     * This method initializes and returns an AWS.DynamoDB instance
     * @returns an AWS.DynamoDB instance
     */
    ddb() {
        if (this.dynamoDB) {
            return this.dynamoDB;
        }
        if (this.endpointURL) {
            debug('Setting DynamoDB to %s', this.endpointURL);
            this.dynamoDB = createLocalDb(this.endpointURL);
        }
        else {
            debug('Getting default DynamoDB');
            this.dynamoDB = new this.AWS.DynamoDB({
                'httpOptions': {
                    'agent': new https_1.default.Agent({
                        'rejectUnauthorized': true,
                        'keepAlive': true
                    })
                }
            });
        }
        return this.dynamoDB;
    }
    /**
     * This method allows you to override the defaults defined at initialization of this instance
     * @param options the accepted options for Dynamoose or Schemas
     */
    setDefaults(options) {
        for (const key in this.defaults) {
            options[key] = typeof options[key] === 'undefined' ? this.defaults[key] : options[key];
        }
        this.defaults = options;
    }
    /**
     * This method allows you to override the default AWS.DynamoDB instance
     * @param ddb an instance of AWS.DynamoDB
     */
    setDDB(ddb) {
        debug('Setting custom DDB');
        this.dynamoDB = ddb;
    }
    /**
     * This method allows you to clear the AWS.DynamoDB instance
     */
    revertDDB() {
        debug('Reverting to default DDB');
        this.dynamoDB = undefined;
    }
    /**
     * This method process an array of models as defined by the options and calls the callback when complete
     * @param items An array of Models to process
     * @param options Either a callback or the allowed option set
     * @param next A callback for post transaction completion
     */
    async transaction(items, options, next) {
        debugTransaction('Run Transaction');
        const deferred = q_1.default.defer();
        const dbClient = this.documentClient();
        const DynamoDBSet = dbClient.createSet([1, 2, 3]).constructor;
        const that = this;
        let builtOptions = options || {};
        if (typeof options === 'function') {
            next = options;
            builtOptions = {};
        }
        if (!Array.isArray(items) || items.length === 0) {
            deferred.reject(new errors_1.default.TransactionError('Items required to run transaction'));
            return deferred.promise.nodeify(next);
        }
        const tmpItems = await Promise.all(items);
        items = tmpItems;
        const transactionReq = {
            'TransactItems': items.map(i => {
                // Omit the dynamoose model reference.
                const itemCopy = Object.assign({}, i);
                delete itemCopy._dynamooseModel;
                return itemCopy;
            })
        };
        let transactionMethodName;
        if (builtOptions.type) {
            debugTransaction('Using custom transaction method');
            if (builtOptions.type === 'get') {
                transactionMethodName = 'transactGetItems';
            }
            else if (builtOptions.type === 'write') {
                transactionMethodName = 'transactWriteItems';
            }
            else {
                deferred.reject(new errors_1.default.TransactionError('Invalid type option, please pass in "get" or "write"'));
                return deferred.promise.nodeify(next);
            }
        }
        else {
            debugTransaction('Using predetermined transaction method');
            transactionMethodName = items.map((obj) => Object.keys(obj)[0]).every((key) => key === 'Get') ? 'transactGetItems' : 'transactWriteItems';
        }
        debugTransaction(`Using transaction method: ${transactionMethodName}`);
        const transact = () => {
            debugTransaction('transact', transactionReq);
            this.dynamoDB[transactionMethodName](transactionReq, async (err, data) => {
                if (err) {
                    debugTransaction(`Error returned by ${transactionMethodName}`, err);
                    return deferred.reject(err);
                }
                debugTransaction(`${transactionMethodName} response`, data);
                if (!data.Responses) {
                    return deferred.resolve();
                }
                return deferred.resolve((await Promise.all(data.Responses.map(async (item, index) => {
                    const { TheModel, schema } = getModelSchemaFromIndex(items[index], this);
                    Object.keys(item).forEach((prop) => {
                        if (item[prop] instanceof DynamoDBSet) {
                            item[prop] = item[prop].values;
                        }
                    });
                    const model = new TheModel();
                    model.$__.isNew = false;
                    // Destruct 'item' DynamoDB's returned structure.
                    await schema.parseDynamo(model, item.Item);
                    debugTransaction(`${transactionMethodName} parsed model`, model);
                    return model;
                }))).filter((item, index) => {
                    const { schema } = getModelSchemaFromIndex(items[index], this);
                    return !(schema.expires && schema.expires.returnExpiredItems === false && item[schema.expires.attribute] && item[schema.expires.attribute] < new Date());
                }));
            });
        };
        if (builtOptions.returnRequest) {
            deferred.resolve(transactionReq);
        }
        else if (items.some((item, index) => getModelSchemaFromIndex(items[index], this).TheModel.$__.table.options.waitForActive)) {
            const waitForActivePromises = Promise.all(items.filter((item, index) => getModelSchemaFromIndex(items[index], this).TheModel.$__.table.options.waitForActive).map((item, index) => getModelSchemaFromIndex(items[index], this).TheModel.$__.table.waitForActive()));
            waitForActivePromises.then(transact).catch(deferred.reject);
        }
        else {
            transact();
        }
        return deferred.promise.nodeify(next);
    }
}
const DynamooseInstance = new Dynamoose();
exports.default = DynamooseInstance;
