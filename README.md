
Run python scripts as k8s jobs.

## Install

1. clone repo
2. run `pip install e .`


## Run

```shell 

pyk8sjob submit myscript.py --image python:3.7.13-slim-buster
```

To create presets to set tolerations, node_selector and resources, run `pyk8sjob presets edit` which will open the default editor. 
You can then add, for example: 

```yaml

spotLarge:
  image: python:3.7.13-slim-buster
  tolerations:
      - key: spotInstance
        operator: Exists
        effect: NoSchedule
      - key: size
        operator: Exists
        effect: NoSchedule
  node_selector:
     spotInstance: "true"
     size: large
  resources:
     requests:
        memory: 6Gi
large:
  tolerations:
      - key: size
        operator: Exists
        effect: NoSchedule
  node_selector:
     size: large
  resources:
     requests:
        memory: 6Gi
```

Now you can run using a preset

```bash
pyk8sjob submit myscript.py --preset large 
```

see `pyk8sjob --help`