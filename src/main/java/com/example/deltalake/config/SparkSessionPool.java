package com.example.deltalake.config;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.commons.pool2.BasePooledObjectFactory;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

@Configuration
public class SparkSessionPool {
    private final GenericObjectPool<SparkSession> pool;
    private final ReentrantLock lock = new ReentrantLock();
    public SparkSessionPool(
            @Value("${s3a.URL}") String s3aUrl,
            @Value("${s3a.access.key}") String accessKey,
            @Value("${s3a.secret.key}") String secretKey,
            @Value("${maxSparkSessionInstances:10}") Integer maxSparkSessionInstances
    )
    {
        SparkSession sparkSession = SparkSession.builder()
                .appName("DeltaLakeSparkMinio")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
                .config("spark.hadoop.fs.s3a.access.key", Objects.requireNonNull(accessKey))
                .config("spark.hadoop.fs.s3a.secret.key", Objects.requireNonNull(secretKey))
                .config("spark.hadoop.fs.s3a.endpoint", Objects.requireNonNull(s3aUrl))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.master", "local[*]")
                .getOrCreate();

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(maxSparkSessionInstances); // maximum number of SparkSession instances in the pool
        pool = new GenericObjectPool<>(new SparkSessionFactory(sparkSession), poolConfig);
    }

    @Nullable
    public SparkSession borrowSparkSession() throws Exception {
        lock.lock();
        try {
            return pool.borrowObject();
        } finally {
            lock.unlock();
        }
    }

    public void returnSparkSession(SparkSession sparkSession) {
        lock.lock();
        try {
            pool.returnObject(sparkSession);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        pool.close();
    }
    @PreDestroy
    public void onDestroy() {

        if (pool != null) {
            close();
        }
    }

    private static class SparkSessionFactory extends BasePooledObjectFactory<SparkSession> {

        private final SparkSession spark;

        public SparkSessionFactory(SparkSession spark) {
            this.spark = spark;
        }

        @Override
        public SparkSession create(){
            return spark.newSession();
        }

        @Override
        public PooledObject<SparkSession> wrap(SparkSession sparkSession) {
            return new DefaultPooledObject<>(sparkSession);
        }

        @Override
        public void destroyObject(PooledObject<SparkSession> pooledObject){
            SparkSession sparkSession = pooledObject.getObject();
            sparkSession.stop();
        }
    }
}
