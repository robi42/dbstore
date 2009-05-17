/**
 * Module for using Hibernate as ORM/persistence layer.
 */

addToClasspath('./lib/antlr-2.7.6.jar');
addToClasspath('./lib/c3p0-0.9.1.jar');
addToClasspath('./lib/commons-collections-3.1.jar');
addToClasspath('./lib/commons-logging-1.1.1.jar');
addToClasspath('./lib/dom4j-1.6.1.jar');
addToClasspath('./lib/ehcache-1.2.3.jar');
addToClasspath('./lib/hibernate3.jar');
addToClasspath('./lib/javassist-3.4.GA.jar');
addToClasspath('./lib/jta-1.1.jar');

importPackage(org.hibernate.cfg);
importPackage(org.hibernate.proxy.map);

export('Storable', 'getSession', 'beginTxn', 'commitTxn');

var Storable = require('../storable').Storable;
Storable.setStoreImplementation(this);

var __shared__ = true;
var log = require('helma/logging').getLogger(__name__);

var configPropsFileRelativePath = 'config/hibernate.properties';
var mappingsDirRelativePath = 'db/mappings';
var config, isConfigured = false;
var sessionFactory;

/**
 * Begins a Hibernate Session transaction.
 */
function beginTxn(session) {
    try {
        var sess = session || getSession();
        var txn = sess.beginTransaction();
    } catch (e) {
        txn.rollback();
        log.error('Error in beginTxn: ' + e.message);
        throw e;
    }
}

/**
 * Commits a Hibernate Session transaction.
 */
function commitTxn(session) {
    try {
        var sess = session || getSession();
        var txn = sess.transaction;
        txn.commit();
    } catch (e) {
        txn.rollback();
        log.error('Error in commitTxn: ' + e.message);
        throw e;
    }
}

/**
 * Gets a Hibernate DB session.
 */
function getSession() {
    if (!isConfigured) {
        configure();
    }
    return sessionFactory.currentSession;
}

/**
 * Sets basic Hibernate configuration.
 */
function configure() {
    var mappingsDirAbsolutePath = getResource(mappingsDirRelativePath).path;
    var configPropsFileAbsolutePath = getResource(configPropsFileRelativePath).path;
    var configPropsFile = new java.io.File(configPropsFileAbsolutePath);
    var fileInputStream = new java.io.FileInputStream(configPropsFile);
    var configProps = new java.util.Properties();

    // load hibernate.properties
    configProps.load(fileInputStream);
    fileInputStream.close();

    config = new Configuration();
    // add mappings dir
    config.addDirectory(new java.io.File(mappingsDirAbsolutePath));
    // set properties from hibernate.properties file
    config.setProperties(configProps);
    // use dynamic-map entity persistence mode
    config.setProperty('hibernate.default_entity_mode', 'dynamic-map');
    // transactions are handled by JDBC, no JTA is used
    config.setProperty('hibernate.transaction.factory_class',
            'org.hibernate.transaction.JDBCTransactionFactory');
    // enable session binding to managed context
    config.setProperty('hibernate.current_session_context_class', 'thread');
    // enable the second level query cache
    config.setProperty('hibernate.cache.use_query_cache', 'true');
    // use easy hibernate (eh) cache
    config.setProperty('hibernate.cache.provider_class',
            'org.hibernate.cache.EhCacheProvider');
    // use c3p0 connection pooling
    config.setProperty('hibernate.connection.provider_class',
            'org.hibernate.connection.C3P0ConnectionProvider');
    isConfigured = true;
    sessionFactory = config.buildSessionFactory();
}

function all(type) {
    var session = getSession();
    beginTxn(session);
    var criteria = session.createCriteria(type);
    criteria.setCacheable(true);
    var i, result = new ScriptableList(criteria.list());
    for (i in result) {
        result[i] = new Storable(result[i].$type$, new ScriptableMap(result[i]));
    }
    commitTxn(session);
    return result;
}

function get(type, id) {
    var session = getSession();
    beginTxn(session);
    var result = session.get(new java.lang.String(type), new java.lang.Long(id));
    if (result != null) {
        result = new Storable(type, new ScriptableMap(result));
    }
    commitTxn(session);
    return result;
}

function save(props, entity, entities) {
    if (entities && entities.contains(entity)) {
        return;
    }
    var isRoot = false;
    if (!entities) {
        isRoot = true;
        entities = new java.util.HashSet();
        beginTxn();
    }
    entities.add(entity);
    for (var id in props) {
        var value = props[id];
        if (isStorable(value)) {
            value.save(entities);
            value = value._key;
        }
        entity[id] = value;
    }
    if (isRoot) {
        var session = getSession();
        var obj, i;
        for (i = 0; i < entities.size(); i++) {
            obj = entities.toArray()[i];
            session['saveOrUpdate(java.lang.String,java.lang.Object)']
                    (obj.$type$, obj);
        }
        commitTxn(session);
    }
}

function getProps(type, arg) {
    if (arg instanceof Object) {
        arg.$type$ = type;
        return arg;
    } else if (isEntity(arg)) {
        var props = {};
        var i, map = new ScriptableMap(arg);
        for (i in map) {
            props[i] = map[i];
        }
        return props;
    }
    return null;
}

function getEntity(type, arg) {
    if (isEntity(arg)) {
        return arg;
    } else if (arg instanceof Object) {
        var entity = new ScriptableMap(new java.util.HashMap(arg));
        entity.$type$ = type;
        return entity;
    }
    return null;
}

function isEntity(value) {
    return value instanceof MapProxy;
}

function isStorable(value) {
    return value instanceof Storable;
}
