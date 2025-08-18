namespace SP;

entity USER {
    userId          : String;
    userStatus      : String;    // Active or Inactive
    companyName     : String;
    key email       : String;    // Primary Key
    phoneNumber     : String;
    customerNumber  : String;
    country         : String;
    city            : String;
    postalCode      : String;
    street          : String;
    firstName       : String;
    lastName        : String;
    language        : String;
    expiryDate      : Date;
    department      : String;    // Optional
    additionalNote  : String;    // Optional
}

