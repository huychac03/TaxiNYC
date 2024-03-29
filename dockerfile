FROM prefecthq/prefect:2.8.7-python3.10
#chỉ ra phiên bản nào của prefect và python cần dùng
COPY docker-requirements.txt .
#copy file yêu cầu để khi run nó sẽ tải những thứ cần thiết xuống
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir 
#Lệnh để tải các packages trong file trên
COPY flows /opt/prefect/flows
COPY data-yellow /opt/prefect/data-yellow
#dùng để copy 1 file hoặc folder từ máy sang 
#1 địa chỉ cụ thể trong  Docker image being built


