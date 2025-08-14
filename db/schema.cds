namespace SP;

entity USERS {
  key userid    : String(36);       // Unique user ID
  firstname     : String(100);      // First name
  lastname      : String(100);      // Last name
  email         : String(255);      // Email address
  department    : String(100);      // Department
  customer      : String(100);      // Customer
  country       : String(100);      // Country
  city          : String(100);      // City
}
