# node-js-db
Node js based general purpose database


# Usage

```javascript

const Database   = require('./database');

(async function() {
  const data = {
    username: "test",
    avatar  : "./images/avatar.jpg",
    created : 123456789,
    posts   : 543,
    email   : "example@example.com",
    bio     : "Proin quis vulputate neque. Duis sit amet varius dui. Proin ultricies sit amet velit ac auctor. Mauris leo elit, mollis id urna ut, maximus accumsan quam. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Donec faucibus porttitor lobortis. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin at lectus id eros tempor accumsan eu ut lorem."
  };
  const db         = new Database({
    location: Type String (directory path) // required, path will be created if doesn't exist.
    readonly: Type Boolean
    keys    : Type Array[String] // required for new databases, example ["username", "email"]
  });
  
  // Key selection is important. Selecting the object property data.bio from above is not recommended.
  
  
  await db.open();
  await db.set(data);
  
  let user = await db.select({username: "test"});
  
  void console.log(user);
  void process.exit();
})();

```
