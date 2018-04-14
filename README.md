# Streaming Data Deep Dive

## Requirements

* OpenShift 3.7.2
* Maven
* NodeJS 4.2 (
[`nvm`](https://github.com/creationix/nvm)
is a great tool for switching around NodeJS versions)

## Starting the solution

1. Launch `./start-solution.sh` and verify all the components are correctly deployed
  * Connect to `https://127.0.0.1:8443/` with `developer:developer`
  * Go to `my-project` and these applications with a single pod correctly started should be present :
    - datagrid-visualizer
    - delayed-listener
    - delayed-trains
    - positions-injector
    - stations-injector
    - workshop-main
    - datagrid (with 3 pods)
    
2. Launch the data injectors 
  * Start the injector 
    `curl http://workshop-main-myproject.127.0.0.1.nip.io/inject`

3. Check the datagrid visualizer
  * Go to http://datagrid-visualizer-myproject.127.0.0.1.nip.io/infinispan-visualizer/
  * Select `train-positions` cache. You should see data
  * Select `stations-boards` cache. You should see data
  
4. Start the Dashboard - run the main delayed-dashboard/DelayedDashboard and see the delayed trains

5. Start the node client in the web-viewer : `./start.sh` and see in `http://localhost:3000/` the trains moving

## Live coding

Live coding steps are specific to an event delivery:

* [JFokus live coding](live-coding/dd-jfokus18.org)
