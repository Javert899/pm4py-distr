class MasterVariableContainer:
    port = -1
    master = None
    dbmanager = None
    first_loading_done = False
    log_assignment_done = False
    slave_loading_requested = False
    master_initialization_done = False
    assign_request_threads = []


