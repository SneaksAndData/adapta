import requests


def init_lk():
    # boostrap
    resp = requests.post(url="http://localhost:20001/management/v1/bootstrap", json={"accept-terms-of-use": True})
    if resp.status_code not in [200, 400]:
        resp.raise_for_status()

    # create warehouse
    resp = requests.post(
        url="http://localhost:20001/management/v1/warehouse",
        json={
            "warehouse-name": "demo",
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": "tmp",
                "key-prefix": "initial-warehouse",
                "assume-role-arn": None,
                "endpoint": "http://localhost:9000",
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "minio",
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minioadmin",
                "aws-secret-access-key": "minioadmin",
            },
        },
    )

    if resp.status_code not in [409, 400, 200, 201]:
        resp.raise_for_status()

    # add namespace
    warehouse_prefix = requests.get("http://localhost:20001/catalog/v1/config?warehouse=demo").json()["defaults"][
        "prefix"
    ]
    ns_resp = requests.post(
        f"http://localhost:20001/catalog/v1/{warehouse_prefix}/namespaces", json={"namespace": ["test"]}
    )
    if ns_resp.status_code not in [409, 200, 201]:
        ns_resp.raise_for_status()

    # create second warehouse
    resp_alt = requests.post(
        url="http://localhost:20001/management/v1/warehouse",
        json={
            "warehouse-name": "demo_alt",
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": "tmp",
                "key-prefix": "secondary-warehouse",
                "assume-role-arn": None,
                "endpoint": "http://localhost:9000",
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "minio",
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "aws-access-key-id": "minioadmin",
                "aws-secret-access-key": "minioadmin",
            },
        },
    )

    if resp_alt.status_code not in [409, 400, 200, 201]:
        resp_alt.raise_for_status()

    # add namespace in second warehouse
    alt_warehouse_prefix = requests.get("http://localhost:20001/catalog/v1/config?warehouse=demo_alt").json()[
        "defaults"
    ]["prefix"]
    alt_ns_resp = requests.post(
        f"http://localhost:20001/catalog/v1/{alt_warehouse_prefix}/namespaces", json={"namespace": ["test_b"]}
    )
    if alt_ns_resp.status_code not in [409, 200, 201]:
        alt_ns_resp.raise_for_status()


if __name__ == "__main__":
    init_lk()
