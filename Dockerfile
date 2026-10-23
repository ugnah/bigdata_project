# Sử dụng Ubuntu 22.04 làm nền tảng
FROM ubuntu:22.04

# Tránh hỏi tương tác khi cài đặt
ENV DEBIAN_FRONTEND=noninteractive

# Cài đặt Java 11, Python 3.10, pip
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Tạo symlink python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Cài đặt PySpark 3.5.0 (ổn định, tương thích tốt)
RUN pip3 install pyspark==3.5.0

# Tạo thư mục làm việc
WORKDIR /app

# Copy toàn bộ project vào container
COPY . /app

# Giữ container chạy
CMD ["bash"]