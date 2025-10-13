namespace SPUSER_STAGING;

type StatusEnum : String(8) enum {
  ACTIVE = 'active';
  INACTIVE = 'inactive';
};
 
type UserTypeEnum : String(8) enum {
  PUBLIC = 'public';
  INTERNAL = 'internal';
};

entity P_USERS {
    key userUuid     : UUID;
        userId       : String(255) @assert.format: '^P[0-9]+$';
        firstName    : String(255);
        lastName     : String(255);
        displayName  : String(255);
        email        : String(255) @assert.format: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$';

        @cds.nullable: true
        phoneNumber  : String(16);

        @cds.nullable: true
        country      : String(255);

        @cds.nullable: true
        zip          : String(12);

        userName     : String(255);

        status       : StatusEnum;
        userType     : UserTypeEnum;

        @cds.nullable: true
        mailVerified : Boolean;

        @cds.nullable: true
        phoneVerified: Boolean;

        @cds.nullable: true
        created      : Timestamp;  

        @cds.nullable: true
        lastModified : Timestamp;

        @cds.nullable: true
        modifiedBy   : String(255);
}

entity CRM_COMPANY_ACCOUNTS {
  key uuid            : UUID;
      accountId       : Integer not null;
      accountName     : String(255);
      erpNo           : String(255);
      crmToErpFlag    : Boolean;
      contacts        : Composition of many CRM_COMPANY_CONTACTS on contacts.accountId = $self.accountId;
      erpCustomer     : Association to ERP_CUSTOMERS on erpCustomer.crmBpNo = $self.accountId;
}
 
 
entity CRM_COMPANY_CONTACTS {
  key uuid            : UUID;
      contactId       : Integer not null;
      accountId       : Integer not null;
      erpContactPerson: String(255);
      firstName       : String(255);
      lastName        : String(255);
      email           : String(255) @assert.format: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$';

      @cds.nullable: true
      department      : String(255);
      country         : String(255);
      zipCode         : String(12);
      company         : Association to CRM_COMPANY_ACCOUNTS on company.accountId = $self.accountId;
      erpContact      : Association to ERP_CUSTOMERS_CONTACTS on erpContact.contactPersonId = $self.erpContactPerson;
}
 
 
entity ERP_CUSTOMERS {
  key uuid            : UUID;
      customerId      : String(255);
      name            : String(255);
      crmBpNo         : Integer not null;
      crmCompany      : Association to CRM_COMPANY_ACCOUNTS on crmCompany.accountId = $self.crmBpNo;
      contacts        : Composition of many ERP_CUSTOMERS_CONTACTS on contacts.customerId = $self.customerId;
}
 
 
entity ERP_CUSTOMERS_CONTACTS {
  key uuid            : UUID;
      contactPersonId : String(255);
      customerId      : String(255);
      firstName       : String(255);
      lastName        : String(255);
      email           : String(255) @assert.format: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$';

      @cds.nullable: true
      department      : String(255);
      country         : String(255);

      @cds.nullable: true
      cshmeFlag       : Boolean;
      phoneNo         : String(16);
      status          : StatusEnum;

      @cds.nullable: true
      createdAt       : Timestamp;
      customer        : Association to ERP_CUSTOMERS on customer.customerId = $self.customerId;
}
