from prefect.deployments import Deployment
# ta dùng phương thức deployment của Prefect để deploy

from etl_parameterized_flow  import etl_parent_flow
# gọi đến flow cần sử dụng để lưu và làm sạch dữ liệu
# mỗi file và mỗi lần làm ta sẽ có những tênh file etl và các flow khác nhau 
# etl_parameterized_flow là tên file etl 
# etl_parent_flow là flow chính

from prefect.infrastructure.docker import DockerContainer
docker_container_block = DockerContainer.load("docker-zoomcamp")
# ta copy phần này từ Blocks vào 
# (ta đã cấu hình sẵn cho nó sẽ sử dụng Image huychac03/prefect:zoomcamp)

docker_dep = Deployment.build_from_flow( # sử dụng build from flow trong deployment
    flow= etl_parent_flow, # flow chính
    name="docker-flow-new",  # tên ta muốn đặt cho Deployment này 
    infrastructure=docker_container_block  # infrastructure chính là Block mà ta đã dựng
)

if __name__ == "__main__":
    docker_dep.apply()
# luôn luôn phải có main và apply để có thể chạy


        