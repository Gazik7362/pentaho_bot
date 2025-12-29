import json
import logging
from config.settings import USERS_FILE_PATH

class AuthService:
    def __init__(self):
        self.users = {}
        self.roles = {}
        self.reload()

    def reload(self):
        """Reloads users and roles from JSON without restarting the bot."""
        try:
            with open(USERS_FILE_PATH, 'r') as f:
                data = json.load(f)
                self.users = {int(k): v for k, v in data['users'].items()}
                self.roles = data['roles']
            logging.info("AuthService: User DB loaded.")
        except Exception as e:
            logging.error(f"AuthService Error: {e}")

    def get_role(self, user_id):
        return self.users.get(user_id)

    def has_permission(self, user_id, action):
        role = self.get_role(user_id)
        if not role: 
            return False
        return action in self.roles.get(role, [])

    def add_user(self, user_id, role):
        """Adds a new user and saves to JSON."""
        try:
            # Update memory
            self.users[int(user_id)] = role
            
            # Update file
            with open(USERS_FILE_PATH, 'r') as f:
                data = json.load(f)
            
            data['users'][str(user_id)] = role
            
            with open(USERS_FILE_PATH, 'w') as f:
                json.dump(data, f, indent=4)
            
            logging.info(f"AuthService: Added user {user_id} as {role}")
            return True
        except Exception as e:
            logging.error(f"AuthService Save Error: {e}")
            return False    

# Create a singleton instance
auth_service = AuthService()
