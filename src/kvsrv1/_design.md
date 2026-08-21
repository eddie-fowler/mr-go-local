Goal
    - Implement a single machine key/value store that provides an at most once guarantee *
    - Operations are linear * 
    - Non-concurrent operations will result in a state changes that match the sequence of the put/get requests
    - Concurrent operations will result in state that matches some sequence of the commmands 
Modules 
    - client.go
      - Sends RPC get/put requests to server
      - handles responses from server 
        - success 
        - failure
      - Retries requests until success
    - server.go
      - Manages key/value state 
      - linearizes requests 
        - Can use channel buffer to absorb requests then process
        - mutex locks also provide linearization (only one goroutine/thread has access at a time until unlock initiated)
    - rpc.go
State
    -  KeyValue
       -  Key
       -  Value
       -  Version
       -  Queue chan
