import asyncio

from rdf4j_python import AsyncRdf4jDB


async def main():
    async with AsyncRdf4jDB("http://localhost:19780/rdf4j-server") as db:
        repositories = await db.list_repositories()
        print("Repositories:", repositories)

        version = await db.get_protocol_version()
        print("Protocol Version:", version)

        repo_config = """
            @prefix config: <tag:rdf4j.org,2023:config/>.
            [] a config:Repository ;
            config:rep.id "example-repo" ;
            config:rep.impl [
                config:rep.type "openrdf:SailRepository" ;
                config:sail.impl [
                    config:sail.type "openrdf:MemoryStore" ;
                ]
            ] .
        """
        await db.create_repository(
            repository_id="example-repo",
            rdf_config_data=repo_config,
            content_type="application/x-turtle",
        )
        print("Repository 'example-repo' created.")
        # List repositories again to confirm creation
        repositories = await db.list_repositories()
        print("Repositories after creation:", repositories)


if __name__ == "__main__":
    asyncio.run(main())
