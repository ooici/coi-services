from pyon.net.endpoint import RPCClient
from interface.services.ibank_service import IBankService

def run_client(container):
    """
    This method will establish an RPC client endpoint to the Bank service and send a series of requests.
    """
    client = RPCClient(node=container.node, name="bank", iface=IBankService)
    print 'RPC endpoint created'

    print 'Creating savings account'
    savingsAcctNum = client.new_account('kurt', 'Savings')
    print "New savings account number: " + str(savingsAcctNum)
    print "Starting savings balance %s" % str(client.get_balances(savingsAcctNum))
    client.deposit(savingsAcctNum, 99999999)
    print "Savings balance after deposit %s" % str(client.get_balances(savingsAcctNum))
    client.withdraw(savingsAcctNum, 1000)
    print "Savings balance after withdrawl %s" % str(client.get_balances(savingsAcctNum))

    print "Buying 1000 savings bonds"
    client.buy_bonds(savingsAcctNum, 1000)
    print "Savings balance after bond purchase %s" % str(client.get_balances(savingsAcctNum))

    checkingAcctNum = client.new_account('kurt', 'Checking')
    print "New checking account number: " + str(checkingAcctNum)
    print "Starting checking balance %s" % str(client.get_balances(checkingAcctNum))
    client.deposit(checkingAcctNum, 99999999)
    print "Confirming checking balance after deposit %s" % str(client.get_balances(checkingAcctNum))
    client.withdraw(checkingAcctNum, 1000)
    print "Confirming checking balance after withdrawl %s" % str(client.get_balances(checkingAcctNum))

    acctList = client.list_accounts('kurt')
    for acct_obj in acctList:
        print "Account: " + str(acct_obj)
