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

import('helma.system', 'system');
var {partial} = require('helma.functional');
var log = require('helma.logging').getLogger(__name__);

var __shared__ = true;


// used for holding the Store instance
var store;
this.initStore();


(function () {

   // used to get paths of hibernate.properties and mapping files
   var configPropsFileRelativePath;
   var mappingsDirRelativePath = 'db/mappings';

   var isConfigured = false;
   var config, sessionFactory;


   /**
    * Use this for setting the path in which hibernate.properties file resides.
    */
   this.setConfigPath = function (path) {
      configPropsFileRelativePath = path + '/hibernate.properties';
   };


   /**
    * Use this for setting the path where the *.hbm.xml mapping files resides.
    */
   this.setMappingsDir = function (path) {
      mappingsDirRelativePath = path;
   };


   /**
    * Adds callbacks for binding Hibernate
    * DB session transactions to request scope;
    * call this in the __main__ scope of an app.
    */
   this.addTxnCallbacks = function () {

      system.addCallback('onRequest', 'beginHibernateTxn', function () {
         beginTxn();
      });

      system.addCallback('onResponse', 'commitHibernateTxn', function () {
         commitTxn();
      });
   };


   /**
    * Begins a Hibernate Session transaction.
    */
   this.beginTxn = function () {
      try {
         var sess = getSession();
         var txn = sess.beginTransaction();
      } catch (e) {
         txn.rollback();
         log.error('in onRequest beginHibernateTxn callback: ' + e.toString());
      }
   };


   /**
    * Commits a Hibernate Session transaction.
    */
   this.commitTxn = function () {
      try {
         var sess = sessionFactory.getCurrentSession();
         var txn = sess.getTransaction();
         txn.commit();
      } catch (e) {
         txn.rollback();
         log.error('in onResponse commitHibernateTxn callback: ' + e.toString());
      }
   };


   /**
    * Gets a Hibernate DB session.
    */
   this.getSession = function () {
      if (!isConfigured) {
         setConfig();
      }

      return sessionFactory.getCurrentSession();
   };


   /**
    * Sets the basic Hibernate configuration.
    */
   var setConfig = function () {
      var mappingsDirAbsolutePath = getResource(mappingsDirRelativePath).path;
      var configPropsAbsolutePath = getResource(configPropsFileRelativePath).path;
      var configPropsFile = new java.io.File(configPropsAbsolutePath);
      var fileInputStream = new java.io.FileInputStream(configPropsFile);
      var configProps = new java.util.Properties();

      // load hibernate.properties
      configProps.load(fileInputStream);
      fileInputStream.close();

      // set configuration
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
      log.info('Configuration set.');

      sessionFactory = config.buildSessionFactory();
   };


   /**
    * Use this for rebuilding the DB schema from the *.hbm.xml mapping files.
    */
   this.rebuildDbSchema = function () {
      setConfig();

      new org.hibernate.tool.hbm2ddl.SchemaExport(config).setOutputFile('db/schema.sql')
                                                         .execute(true, true, false, false);
   };


   /**
    * To be used for wrapping model objects into (decorated) ScriptableMaps to enable persistance.
    */
   this.makeStorable = function (object, properties) {
      if (!(object instanceof Object) || !(object.constructor instanceof Function)) {
         throw new Error('object must be an object, is: ' + object);
      }
      if (!(object.constructor instanceof Function)) {
         throw new Error('object must have a constructor property, has: ' + object.constructor);
      }

      if (properties === undefined) {
         properties = {};
      }
      if (!(properties instanceof Object)) {
         throw new Error('properties must be an object, is: ' + properties);
      }

      var typeName = object.constructor.name;
      if (typeof typeName != 'string') {
         throw new Error("couldn't get type: " + typeName);
      }

      var scriptableMap = new ScriptableMap(new java.util.HashMap(object));

      // add all properties and set $type$, accordingly
      for (var i in properties) {
         scriptableMap[i] = properties[i];
      }
      scriptableMap.$type$ = typeName;

      log.debug(typeName + ' constructed.');

      scriptableMap = this.addDaoMethods(scriptableMap);

      return scriptableMap;
   };


   /**
    * Used for decorating model objects with save() and remove() DAO convenience methods.
    */
   this.addDaoMethods = function (object) {

      object.save = function () {
         store.save(object);
      };

      object.remove = function () {
         store.remove(object);
      };

      return object;
   };

   /**
    * Used for decorating model objects, fetched from DB, with resp. instance methods.
    */
   this.addInstanceMethods = function (object) {
      var constructor = store.typeRegistry[object.$type$];
      var instance = new constructor();

      for (i in instance) {
         if (i != '$type$') {
            object[i] = instance[i];
         }
      }

      return this.addDaoMethods(object);
   };

}).call(this);


/**
 * To be used for installing DAO functionality methods on model constructors and
 * registering types for being able to decorate corresponding model objects with
 * resp. instance methods when fetched from DB.
 */
function Store() {
   this.typeRegistry = {};

   this.registerType = function (constructor) {
      var typeName = constructor.name;

      // install get, find, all and list methods on constructor
      constructor.get  = partial(this.get, typeName);
      constructor.find = partial(this.find, typeName);
      constructor.all  = partial(this.getAll, typeName);
      constructor.list = partial(this.list, typeName);

      // add type to registry
      this.typeRegistry[typeName] = constructor;

      log.debug(typeName + ' type registered.');
   };


   /**
    * Hibernate session API wrapper methods.
    */
   this.save = function (object) {
      try {
         return getHibernateTemplate('save', { object: object });
      } catch (e) {
         log.error('in "save": ' + e.toString());
      }
   };

   this.remove = function (object) {
      try {
         return getHibernateTemplate('remove', { object: object });
      } catch (e) {
         log.error('in "remove": ' + e.toString());
      }
   };

   this.get = function (type, id) {
      try {
         return getHibernateTemplate('get', { type: type, id: id });
      } catch (e) {
         log.error('in "get": ' + e.toString());
      }
   };

   this.find = function (type, query) {
      try {
         query = 'from ' + type + ' ' + type.substring(0, 1).toLowerCase() + ' ' + query;
         return getHibernateTemplate('find', { query: query });
      } catch (e) {
         log.error('in "find": ' + e.toString());
      }
   };

   this.getAll = function (type) {
      try {
         return getHibernateTemplate('getAll', { type: type });
      } catch (e) {
         log.error('in "all": ' + e.toString());
      }
   };

   this.list = function (type, params) {
      try {
         params.type = type;
         return getHibernateTemplate('list', params);
      } catch (e) {
         log.error('in "list": ' + e.toString());
      }
   };

   this.query = function (query) {
      try {
         return getHibernateTemplate('createQuery', { query: query });
      } catch (e) {
         log.error('in "query": ' + e.toString());
      }
   };


   /**
    * Template taking care of executing the actual Hibernate session API wrapper method operations.
    */
   var getHibernateTemplate = function (method, params) {
      var result, sess = getSession();

      // do the actual operation
      switch (method) {
         case 'save':
            sess['saveOrUpdate(java.lang.String,java.lang.Object)'](params.object.$type$, params.object);
            break;

         case 'remove':
            sess['delete(java.lang.Object)'](params.object);
            break;

         case 'get':
            result = sess.get(new java.lang.String(params.type), new java.lang.Long(params.id));

            if (result != null) {
               result = this.addInstanceMethods(new ScriptableMap(result));
            }
            break;

         case 'find':
            result = new ScriptableList(sess.find(new java.lang.String(params.query)));

            for (i in result) {
               result[i] = this.addInstanceMethods(new ScriptableMap(result[i]));
            }
            break;

         case 'getAll':
            var criteria = sess.createCriteria(params.type);

            criteria.setCacheable(true);

            result = new ScriptableList(criteria.list());

            for (i in result) {
               result[i] = this.addInstanceMethods(new ScriptableMap(result[i]));
            }
            break;

         case 'list':
            var criteria = sess.createCriteria(params.type);

            criteria.setCacheable(true);

            if (params.orderBy) {
               var order = (params.order == 'asc') ?
                           org.hibernate.criterion.Order.asc(params.orderBy) :
                           org.hibernate.criterion.Order.desc(params.orderBy);

               criteria.addOrder(order);
            }

            if (params.first && (typeof params.first == 'number')) {
               criteria.setFirstResult(params.first);
            }

            if (params.max && (typeof params.max == 'number')) {
               criteria.setMaxResults(params.max);
            }

            if (params.sql) {
               var sqlRestriction = org.hibernate.criterion.Restrictions.sqlRestriction(params.sql);

               criteria.add(sqlRestriction);
            }

            result = new ScriptableList(criteria.list());

            for (i in result) {
               result[i] = this.addInstanceMethods(new ScriptableMap(result[i]));
            }
            break;

         case 'createQuery':
            result = sess.createQuery(new java.lang.String(params.query));
            break;

         default:
            break;
      }

      return result || null;
   };

}

/**
 * Used to initialize store.
 */
function initStore() {
   store = store || new Store();
   log.info('Store initialized with empty typeRegistry: ' + uneval( store.typeRegistry ));
}


/**
 * To be used for handling Hibernate mapping sets in a "Helma-tic" JS style way.
 */
var Set = system.extendJavaClass(java.util.Set);

Set.prototype.helmatize = function () {
   var item, items = [];
   var arraySet = this.toArray();

   for (var i = 0; i < arraySet.length; i++) {
      item = new ScriptableMap(arraySet[i]);
      items[i] = addInstanceMethods(item);
   }

   return items;
};
