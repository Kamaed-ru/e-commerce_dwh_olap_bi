#!/bin/bash
echo ">>> Preparing PXF MinIO config..."

# Создаём каталог для сервера (он уже есть в образе, используем существующий корень)
mkdir -p /data/pxf/servers/minio

# Записываем конфиг Hadoop S3A для MinIO (без лишних EOF)
cat <<EOF > /data/pxf/servers/minio/minio-site.xml
<configuration>
  <property><name>fs.s3a.endpoint</name><value>http://minio:9000</value></property>
  <property><name>fs.s3a.access.key</name><value>minioadmin</value></property>
  <property><name>fs.s3a.secret.key</name><value>minioadmin</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
</configuration>
EOF

# Меняем владельца, чтобы PXF (под gpadmin) мог читать конфиг
chown -R gpadmin:gpadmin /data/pxf/servers/minio

echo ">>> Restarting PXF service..."

# Рестартуем PXF-кластер тем же способом, что у тебя работает вручную (без запроса пароля)
su - gpadmin -c "pxf cluster stop" 2>/dev/null || true
su - gpadmin -c "pxf cluster start" 2>/dev/null || true

echo ">>> PXF ready!"
