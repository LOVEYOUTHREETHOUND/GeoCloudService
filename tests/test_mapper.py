# import src.geocloudservice.db.mapper as mapper
import utils.db.mapper as mapper
import utils.db.oracle as oracle

def test_getIdByStatus():
    result = mapper.getIdByStatus()
    print(result)
    
def test_getDatanameByOrderId():
    idlist = mapper.getIdByStatus()
    for id in idlist:
        result = mapper.getDatanameByOrderId(id[0])
        print(result)

def test_getAllByOrderIdFromOrder(f_orderid):
    Mymapper = mapper.Mapper(oracle.create_pool())
    result = Mymapper.getAllByOrderIdFromOrder(f_orderid)
    print(result)
    
test_getAllByOrderIdFromOrder(1838490410624552962)