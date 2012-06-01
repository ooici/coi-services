import gevent


def looping_call(interval, callable):                                            
    def loop(interval, callable):                                                
        while True:                                                              
            gevent.sleep(interval)                                               
            callable()                                                           
    return gevent.spawn(loop, interval, callable)   
