import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import uuid
import os
import time



# Origination data to be written
origination_dictionary = {
        "originationId": "[origination]",
        "clientId": "[client]",
        "registerDate": "[date]",
        "installments": [{
                "installmentId": "[installment]",
                "dueDate": "[duedate]",
                "installmentValue": "[value]"
        }]
}

class Installment:
        def __init__(self, installmentId, dueDate, installmentValue):
                self.installmentId = installmentId
                self.dueDate = dueDate
                self.installmentValue = installmentValue

        def to_dict(self):
                installmentsJSON = []
                return {
                        "installmentId": str(self.installmentId),
                        "dueDate": str(self.dueDate),
                        "installmentValue": str(installmentValue)
                        }


class Origination:
        def __init__(self, originationId, clientId, registerDate, installments):
                self.originationId = originationId
                self.clientId = clientId
                self.registerDate = registerDate
                self.installments = installments

        def to_dict(self):
                return {
                        "originationId": str(self.originationId),
                        "clientId": str(self.clientId),
                        "registerDate": str(self.registerDate),
                        "installments": [installment.to_dict() for installment in self.installments]
                        }

class Payment:
        def __init__(self, paymentId, installmentId, paymentDate, paymentValue):
                self.paymentId = paymentId
                self.installmentId = installmentId
                self.paymentDate = paymentDate
                self.paymentValue = paymentValue

        def to_dict(self):
                return {
                        "paymentId": str(self.paymentId),
                        "installmentId": str(self.installmentId),
                        "paymentDate": str(self.paymentDate),
                        "paymentValue": str(self.paymentValue)
                        }
 

if __name__ == "__main__":

        originationId = uuid.uuid4()
        dueDate = datetime.now(timezone.utc).date()
        paymentId = uuid.uuid4()
        clientId = uuid.uuid4()
        installmentValue = 100

        installments = []
        payments = []

        for i in range(10):
             installmentId=uuid.uuid4()
             dt = datetime.now(timezone.utc).date() + relativedelta(months=i)
             installment = Installment(installmentId=installmentId,
                                       dueDate=dt,
                                       installmentValue=installmentValue
             )
             installments.append(installment)

             payment = Payment(paymentId=uuid.uuid4(),
                               installmentId=installmentId,
                               paymentDate=dt,
                               paymentValue=installmentValue
             )
             payments.append(payment)


        origination = Origination(originationId=originationId,
                                  clientId=clientId,
                                  registerDate=datetime.now(timezone.utc).date(),
                                  installments=installments
                                )
        
  
        originationJSON = json.dumps(origination.to_dict())
        
        folder_path = "/data"

        origination_path = os.path.join(folder_path, "originations", f"{str(originationId)}.json")
        with open(origination_path, "w") as origination_file:
                origination_file.write(originationJSON)
                print(f"files saved: {origination_path}")

        for payment in payments:
                payment_path = os.path.join(folder_path, "payments", f"{str(payment.paymentId)}.json")
                paymentJSON = json.dumps(payment.to_dict())
                with open(payment_path, "w") as paymentfile:
                        paymentfile.write(paymentJSON)
                        print(f"files saved: {payment_path}")
                time.sleep(5)

                