from prefect.deployments import Deployment

from flows.orchestrator_flow import etl_flow_orchestration
from flows.etl_flow import etl_flow


if __name__ == "__main__":
    # Deployment del flow maestro (agenda diaria)
    deployment_master = Deployment.build_from_flow(
        flow=etl_flow_orchestration,
        name="etl_flow_orchestration",
        work_queue_name="etl-queue",
        description="Flow maestro que lista archivos y lanza subflows distribuidos"
    )

    # Deployment del subflow (procesamiento individual)
    deployment_sub = Deployment.build_from_flow(
        flow=etl_flow,
        name="etl-procesar-archivo",
        work_queue_name="etl-queue",
        description="Subflow que procesa un archivo individual de visitas web"
    )

    deployment_master.apply()
    deployment_sub.apply()




