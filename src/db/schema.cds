namespace staging;

entity PUser {
  key userUuid    : UUID;             // Auto-generated unique UUID key
      userID      : String(254);      // Business user ID (e.g. P1234567)
      firstName   : String(254);
      lastName    : String(254);
      displayName : String(254);
      email       : String(254);
      phoneNumber : Integer;
      country     : String(254);
      zip         : Integer;
      userName    : String(254);
      status      : String(8);
      userType    : String(8);
}

