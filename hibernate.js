/**
 * Module for using Hibernate as ORM/persistence layer.
 */

addToClasspath('lib/antlr-2.7.6.jar');
addToClasspath('lib/c3p0-0.9.1.jar');
addToClasspath('lib/commons-collections-3.1.jar');
addToClasspath('lib/commons-logging-1.1.1.jar');
addToClasspath('lib/dom4j-1.6.1.jar');
addToClasspath('lib/ehcache-1.2.3.jar');
addToClasspath('lib/hibernate3.jar');
addToClasspath('lib/javassist-3.4.GA.jar');
addToClasspath('lib/jta-1.1.jar');
addToClasspath('lib/slf4j-api-1.4.2.jar');
addToClasspath('lib/slf4j-log4j12-1.4.2.jar');

export('Storable');

var Storable = require('../storable').Storable;
Storable.setStoreImplementation(this);

var __shared__ = true;
var log = require('helma/logging').getLogger(__name__);

// used to get paths of hibernate.properties and mapping files
var configPropsFileRelativePath;
var mappingsDirRelativePath = 'db/mappings';
var isConfigured = false;
var config, sessionFactory;
var session = getSession();

/**
 * Use this for setting the path in which hibernate.properties file resides.
 */
function setConfigPath(path) {
    configPropsFileRelativePath = path + '/hibernate.properties';
}

/**
 * Use this for setting the path where the *.hbm.xml mapping files resides.
 */
function setMappingsDir(path) {
    mappingsDirRelativePath = path;
}

/**
 * Begins a Hibernate Session transaction.
 */
function beginTxn() {
    var txn = null;
    try {
        txn = session.beginTransaction();
    } catch (e) {
        txn.rollback();
        log.error('Error in beginTxn: ' + e.toString());
    }
    return txn;
}

/**
 * Commits a Hibernate Session transaction.
 */
function commitTxn() {
    try {
        var txn = session.getTransaction();
        txn.commit();
    } catch (e) {
        txn.rollback();
        log.error('Error in commitTxn: ' + e.toString());
    }
}

/**
 * Gets a Hibernate DB session.
 */
function getSession() {
    if (!isConfigured) {
        configure();
    }
    return sessionFactory.getCurrentSession();
}

/**
 * Sets basic Hibernate configuration.
 */
function configure() {
    var mappingsDirAbsolutePath = getResource(mappingsDirRelativePath).path;
    var configPropsAbsolutePath = getResource(configPropsFileRelativePath).path;
    var configPropsFile = new java.io.File(configPropsAbsolutePath);
    var fileInputStream = new java.io.FileInputStream(configPropsFile);
    var configProps = new java.util.Properties();

    // load hibernate.properties
    configProps.load(fileInputStream);
    fileInputStream.flush();
    fileInputStream.close();

    config = new org.hibernate.cfg.Configuration();
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

function get(type, id) {
    return new Storable(type, new ScriptableMap(session.
            get(new java.lang.String(type), new java.lang.Long(id))));
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
        entity.put(id, value);
    }
    if (isRoot) {
        var obj;
        for (var i = 0; i < entities.size(); i++) {
            obj = entities.toArray()[i];
            session['saveOrUpdate(java.lang.String,java.lang.Object)']
                    (obj.$type$, obj);
        }
        commitTxn();
    }
}

function getProps(type, arg) {
    if (arg instanceof Object) {
        arg.$type$ = type;
        return arg;
    } else if (isEntity(arg)) {
        var props = {};
        var map = new ScriptableMap(arg);
        for (var i in map) {
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
    return value instanceof org.hibernate.proxy.map.MapProxy;
}

function isStorable(value) {
    return value instanceof Storable;
}
