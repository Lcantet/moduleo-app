# hash.py
from streamlit_authenticator.utilities.hasher import Hasher

# Liste de tes mots de passe en clair
passwords = ["ElectrocutE1!", "Maracaibo1!"]

# Génère la liste de hachés bcrypt
hashed_passwords = Hasher.hash_list(passwords)

# Affiche-les
print(hashed_passwords)
