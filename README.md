db.createUser({
  user: "Admin",
  pwd: "Password",
  roles: [{ role: "userAdminAnyDatabase", db: "admin" }]
})

