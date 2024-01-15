package tp6;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.Id;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Metamodel;

import java.lang.reflect.Field;

public class EntityManagerImpl implements EntityManager {
    private Connection connection;

    public EntityManagerImpl() {
        try {
            this.connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");
        } catch (SQLException e) {
            throw new RuntimeException("Erreur de connexion à la base de données", e);
        }
    }

    private static class Pair<K, V> {
        private final K key;
        private final V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    @Override
    public void persist(Object entity) {
        Class<?> entityClass = entity.getClass();
        String sqlCreate = buildCreateSql(entityClass);
        try (PreparedStatement statement = connection.prepareStatement(sqlCreate)) {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Pair<String, List<String>> insertData = buildInsertSql(entityClass);
        String sqlInsert = insertData.getKey();
        List<String> fieldNames = insertData.getValue();

        try (PreparedStatement statement = connection.prepareStatement(sqlInsert, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < fieldNames.size(); i++) {
                Object fieldValue = getFieldValue(entity, fieldNames.get(i));
                statement.setObject(i + 1, fieldValue);
            }
            statement.executeUpdate();
            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    setFieldValue(entity, "id", generatedKeys.getLong(1));
                }
            }
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <T> T merge(T entity) {
        Class<?> entityClass = entity.getClass();

        Pair<String, List<String>> UpdateData = buildUpdateSql(entityClass);
        String sqlUpdate = UpdateData.getKey();
        List<String> fieldNames = UpdateData.getValue();

        try (PreparedStatement statement = connection.prepareStatement(sqlUpdate)) {
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i) != "id") {
                    Object fieldValue = getFieldValue(entity, fieldNames.get(i));
                    statement.setObject(i + 1, fieldValue);
                }
                statement.setObject(fieldNames.size(), getFieldValue(entity, "id"));
            }
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return entity;
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {

        Object entity = null;

        Pair<String, List<String>> SelectData = buildSelectSql(entityClass);
        String sqlSelect = SelectData.getKey();
        List<String> fieldNames = SelectData.getValue();

        try (PreparedStatement statement = connection.prepareStatement(sqlSelect)) {
            statement.setLong(1, (Long) primaryKey);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    entity = entityClass.getDeclaredConstructor().newInstance();
                    for (int i = 0; i < fieldNames.size(); i++) {
                        setFieldValue(entity, fieldNames.get(i), resultSet.getObject(fieldNames.get(i)));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return entityClass.cast(entity);
    }

    private void setFieldValue(Object obj, String fieldName, Object value) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private Object getFieldValue(Object obj, String fieldName) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(obj);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Pair<String, List<String>> buildInsertSql(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("La classe " + entityClass.getName() + " n'est pas une entité");
        }
        String tableName = entityClass.getSimpleName();
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
        List<String> fieldNames = new ArrayList<>();

        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            String columnName;
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                columnName = column.name().isEmpty() ? field.getName() : column.name();
                sql.append(columnName).append(", ");
                fieldNames.add(field.getName());
            }
        }
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (");

        for (int i = 0; i < fieldNames.size(); i++) {
            sql.append("?,");
        }
        sql.setLength(sql.length() - 1);
        sql.append(")");

        return new Pair<>(sql.toString(), fieldNames);
    }

    private String buildCreateSql(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("La classe " + entityClass.getName() + " n'est pas une entité");
        }

        String tableName = entityClass.getSimpleName();
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            String columnName;
            String columnType;
            if (field.isAnnotationPresent(Id.class)) {
                columnName = field.getName();
                columnType = getSqlType(field.getType()) + " PRIMARY KEY";
                sql.append(columnName).append(" ").append(columnType).append(", ");
            } else if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                columnName = column.name().isEmpty() ? field.getName() : column.name();
                columnType = getSqlType(field.getType());
                sql.append(columnName).append(" ").append(columnType).append(", ");
            }

        }

        sql.setLength(sql.length() - 2);
        sql.append(")");

        return sql.toString();
    }

    private Pair<String, List<String>> buildUpdateSql(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("La classe " + entityClass.getName() + " n'est pas une entité");
        }
        String tableName = entityClass.getSimpleName();
        StringBuilder sql = new StringBuilder("UPDATE " + tableName + " SET ");
        List<String> fieldNames = new ArrayList<>();

        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            String columnName;
            String columnType;
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                columnName = column.name().isEmpty() ? field.getName() : column.name();
                sql.append(columnName).append("=? ,");
                fieldNames.add(field.getName());
            }
        }
        sql.setLength(sql.length() - 1);
        sql.append("WHERE id=?");

        return new Pair<>(sql.toString(), fieldNames);
    }

    private Pair<String, List<String>> buildSelectSql(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("La classe " + entityClass.getName() + " n'est pas une entité");
        }
        String tableName = entityClass.getSimpleName();
        StringBuilder sql = new StringBuilder("SELECT * FROM " + tableName);
        List<String> fieldNames = new ArrayList<>();

        Field[] fields = entityClass.getDeclaredFields();
        for (Field field : fields) {
            String columnName;
            if (field.isAnnotationPresent(Column.class)) {
                fieldNames.add(field.getName());
            }
        }
        sql.append(" WHERE id=?");
        return new Pair<>(sql.toString(), fieldNames);
    }

    private String getSqlType(Class<?> javaType) {
        if (Integer.class.isAssignableFrom(javaType) || int.class.isAssignableFrom(javaType)) {
            return "INT";
        } else if (String.class.isAssignableFrom(javaType)) {
            return "VARCHAR(255)";
        } else if (Double.class.isAssignableFrom(javaType) || double.class.isAssignableFrom(javaType)) {
            return "DOUBLE";
        } else if (Long.class.isAssignableFrom(javaType) || long.class.isAssignableFrom(javaType)) {
            return "BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1)";
        }
        throw new IllegalArgumentException("Type non géré : " + javaType.getName());
    }

    @Override
    public void remove(Object entity) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'remove'");
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'find'");
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'find'");
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode, Map<String, Object> properties) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'find'");
    }

    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReference'");
    }

    @Override
    public void flush() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'flush'");
    }

    @Override
    public void setFlushMode(FlushModeType flushMode) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setFlushMode'");
    }

    @Override
    public FlushModeType getFlushMode() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFlushMode'");
    }

    @Override
    public void lock(Object entity, LockModeType lockMode) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lock'");
    }

    @Override
    public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lock'");
    }

    @Override
    public void refresh(Object entity) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refresh'");
    }

    @Override
    public void refresh(Object entity, Map<String, Object> properties) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refresh'");
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refresh'");
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refresh'");
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clear'");
    }

    @Override
    public void detach(Object entity) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'detach'");
    }

    @Override
    public boolean contains(Object entity) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'contains'");
    }

    @Override
    public LockModeType getLockMode(Object entity) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLockMode'");
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setProperty'");
    }

    @Override
    public Map<String, Object> getProperties() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getProperties'");
    }

    @Override
    public Query createQuery(String qlString) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createQuery'");
    }

    @Override
    public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createQuery'");
    }

    @Override
    public Query createQuery(CriteriaUpdate updateQuery) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createQuery'");
    }

    @Override
    public Query createQuery(CriteriaDelete deleteQuery) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createQuery'");
    }

    @Override
    public <T> TypedQuery<T> createQuery(String qlString, Class<T> resultClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createQuery'");
    }

    @Override
    public Query createNamedQuery(String name) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNamedQuery'");
    }

    @Override
    public <T> TypedQuery<T> createNamedQuery(String name, Class<T> resultClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNamedQuery'");
    }

    @Override
    public Query createNativeQuery(String sqlString) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNativeQuery'");
    }

    @Override
    public Query createNativeQuery(String sqlString, Class resultClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNativeQuery'");
    }

    @Override
    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNativeQuery'");
    }

    @Override
    public StoredProcedureQuery createNamedStoredProcedureQuery(String name) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createNamedStoredProcedureQuery'");
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createStoredProcedureQuery'");
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName, Class... resultClasses) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createStoredProcedureQuery'");
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName, String... resultSetMappings) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createStoredProcedureQuery'");
    }

    @Override
    public void joinTransaction() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'joinTransaction'");
    }

    @Override
    public boolean isJoinedToTransaction() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isJoinedToTransaction'");
    }

    @Override
    public <T> T unwrap(Class<T> cls) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'unwrap'");
    }

    @Override
    public Object getDelegate() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDelegate'");
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'close'");
    }

    @Override
    public boolean isOpen() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isOpen'");
    }

    @Override
    public EntityTransaction getTransaction() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTransaction'");
    }

    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEntityManagerFactory'");
    }

    @Override
    public CriteriaBuilder getCriteriaBuilder() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCriteriaBuilder'");
    }

    @Override
    public Metamodel getMetamodel() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMetamodel'");
    }

    @Override
    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createEntityGraph'");
    }

    @Override
    public EntityGraph<?> createEntityGraph(String graphName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createEntityGraph'");
    }

    @Override
    public EntityGraph<?> getEntityGraph(String graphName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEntityGraph'");
    }

    @Override
    public <T> List<EntityGraph<? super T>> getEntityGraphs(Class<T> entityClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEntityGraphs'");
    }
}