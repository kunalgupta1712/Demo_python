# Demo_python
1. crmToErpFlag can be changed from False → True, but not from True → False:

1.1. Changing from True to False would mean the Account will no longer act as a Customer in ERP.
1.2. In such a case, the Account should instead be inactivated, and that inactivation status should be replicated to ERP.
1.3. Setting crmToErpFlag back to False would break the synchronization between CRM and ERP, which is not desired.

2. When an Account is inactivated:

2.1. All associated contacts must also be set as inactive.
2.2. An inactive Account should not have any active contacts.

3. Relationship between crmToErpFlag and chsmeFlag:

3.1. If crmToErpFlag = True, then chsmeFlag can be either True or False.
3.2. If crmToErpFlag = False, then chsmeFlag must also be False, because a True chsmeFlag has no meaning when the Account is not being created or maintained in ERP as Customer.
