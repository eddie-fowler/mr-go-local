Goal – Implement a local MapReduce program 
    - Coordinator to supervise worker processes 
    - Communication between workers and coordinator via RPC calls 

Time constraint – 1 week starting 08/06/2026

Commands 
    - start coordinator [$main go run mrcoordinator.go sock123 pg-*.txt]
    - start worker [$main go run mrworker.go {plugin_filename}.so sock123]

Modules 
    - Coordinator 
      - Create map tasks 
      - Monitor process health 
      - Retry tasks 
      - Store intermediate results to shared memory 
      - merge all intermediate results 
      - partition intermediate results according to number of reducers 
      - create reduce tasks based on reducer count
    - Worker 
      - Map
        - transform (k1, v1) -> list(k1 v1) 
        - store intermediate results in local storage 
        - emit intermediate results to coordinator 
      -  Reduce 
         - reduce (k1, list(v2)) -> list(v2)
         - store output results in local storage 
         - emit to coordinator 
    - RPC
      - RequestTask
      - UpdateTask
    - State Management 
      - IntermediateResults
      - ReducerCount
      - Tasks {id, type, status, assignedWorkerId, contentLocation, retries, createdAt }
      - Actions [Request, Update,]

General Notes
    - mrsequential.go
      - Plugin pattern used to load various MapReduce implementations 
        - Word Counting and Indexing plugins included 
      - Dive deeper into FieldsFunc behavior 
      - General flow partition input -> execute map -> accumulate intermediate -> sort -> reduce across keys -> append line to output
    - mrcoordinator.go
      - need a way to track specific workers and their status 
        - idle, in-progress, completed
      - need a way to queue workers 
        - And requeue 
        - skip thresholds 
      - need to accept RPCs 
    - worker.go
      - Need a way to signal status to Coordinator 
      - Do they need to house both map/reduce functions?
      - Asks for tasks from coordinator until none remain
    - net package
      - listen creates servers 
      - dial connects to servers 
      - rpc
        - provide network access to exported methods of an object 
        - 
    - whole idea is that MapReduce is distributed across "commodity" machines and communicates via network protocols 
      - coordinator and workers will be servers that RPC back and forth 
      - workers ask coordinator for work 
      - execute mapf then reducef -> report back to coordinator 
    - todo
      - write to temp files x 
      - split map and reduce to seprate workers x 
      - allocate task workers by entry params x
      - handle resource locks
        - Use mutex to prevent concurrent updates to coordinator state 
      - handle retries 
      - handle worker heartbeat 
      - handle skip threshold
