from uuid import uuid4


def test_consume_content(
    delete_orphans_pre,
    add_to_cleanup,
    gen_object_with_cleanup,
    container_pull_through_remote_api,
    container_pull_through_distribution_api,
    registry_client,
    local_registry,
    container_repository_api,
    container_remote_api,
    container_distribution_api,
):
    data = {"name": str(uuid4()), "url": "https://registry-1.docker.io"}
    remote = gen_object_with_cleanup(container_pull_through_remote_api, data)

    pull_through_path = str(uuid4())
    data = {"name": str(uuid4()), "base_path": pull_through_path, "remote": remote.pulp_href}
    distribution = gen_object_with_cleanup(container_pull_through_distribution_api, data)

    remote_image_path = "library/busybox"

    registry_client.pull(f"docker.io/{remote_image_path}:latest")
    remote_image = registry_client.inspect(f"docker.io/{remote_image_path}")

    local_registry.pull(f"{distribution.base_path}/{remote_image_path}")

    # clean up a newly created repository, distribution, and remote
    path = f"{pull_through_path}/{remote_image_path}"
    repositories = container_repository_api.list(name=path).results
    add_to_cleanup(container_repository_api, repositories[0].pulp_href)
    remotes = container_distribution_api.list(name=path).results
    add_to_cleanup(container_remote_api, remotes[0].pulp_href)
    distributions = container_remote_api.list(name=path).results
    add_to_cleanup(container_distribution_api, distributions[0].pulp_href)

    local_image = local_registry.inspect(f"{distribution.base_path}/{remote_image_path}")

    assert local_image[0]["Id"] == remote_image[0]["Id"]

    assert 1 == len(repositories)
    assert 1 == len(remotes)
    assert 1 == len(distributions)
