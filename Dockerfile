# Switch to Debian-based image
FROM postgres:15

# Install build dependencies using apt
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    make \
    git \
    unzip \
    postgresql-server-dev-15 \
    clang \
    llvm \
    ca-certificates \
    python3 \
    python3-pip \
    python3-psycopg2 \
    && rm -rf /var/lib/apt/lists/*

# Clone, build, and install the pg_ivm extension
# Adjust PG_CONFIG path if necessary (usually auto-detected better on Debian)
RUN git clone https://github.com/sraoss/pg_ivm.git ./pg_ivm

WORKDIR ./pg_ivm
RUN make && make install
WORKDIR ../
RUN rm -rf pg_ivm

# Copy TPC-H tools into the image (assumes you placed TPC-H-Tool.zip in the build context)
COPY TPC-H-Tool.zip ./tpch.zip
RUN mkdir -p ./tpch && \
    unzip ./tpch.zip -d ./tpch && \
    rm ./tpch.zip

# Configure and compile dbgen
WORKDIR "./tpch/TPC-H V3.0.1/dbgen"
RUN cp makefile.suite makefile && \
    sed -i 's/^MACHINE.*/MACHINE = LINUX/' makefile && \
    sed -i 's/^DATABASE.*/DATABASE = ORACLE/' makefile && \
    sed -i 's/^WORKLOAD.*/WORKLOAD = TPCH/' makefile && \
    make

WORKDIR /
RUN pwd
# Expose PostgreSQL port
EXPOSE 5432

# Optional: Add CMD or ENTRYPOINT to start postgres
# CMD ["postgres"]