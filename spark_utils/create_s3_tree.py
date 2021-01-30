import boto3


def create_s3_tree(bucket, prefix=None):
    """Creates the folder structure of an s3 bucket with a given prefix."""

    bucket = boto3.resource("s3").Bucket(bucket)

    root = {}
    for object_summary in bucket.objects.filter(Prefix=prefix):
        path = object_summary.key
        path = "/".join(path.split("/")[:-1])
        cur = root
        parts = path.split('/')
        for i in range(len(parts)):
            cur = cur.setdefault("/".join(parts[:i+1]), {})

    def detect_partitions(new_root):
        partitions = []
        while True:
            if not new_root.keys():
                break
            result = list(new_root.keys())[0]
            partitions.append(result.split("/")[-1].split("=")[0])
            new_root = new_root[result]
        return partitions

    dirs = []
    def search(root):
        for key in root.keys():
            if root[key] == {}:
                dirs.append(
                    {"path": key, "partition": None})
            elif all("=" in x for x in root[key].keys()):
                dirs.append(
                    {"path": key, "partition": detect_partitions(root[key])})
            else:
                search(root[key])

    search(root)
    return dirs
