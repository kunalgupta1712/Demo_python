namespace SP;

entity USERS {
  userid       : String(36);       // Unique user ID
  firstname    : String(100);      // First name of user
  lastname     : String(100);      // Last name of user
  email        : String(255);      // Email address
  department   : String(100);      // Department name
  customerid   : String(100);      // Customer name or ID
  country      : String(100);      // Country
  city         : String(100);      // City
}
