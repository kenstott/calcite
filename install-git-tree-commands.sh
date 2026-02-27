#!/bin/bash
#
# Install git-tree worktree management commands to ~/bin
#
set -e

mkdir -p ~/bin

echo "Installing git-tree commands to ~/bin..."

# git-tree - Create new worktree
cat > ~/bin/git-tree << 'EOF'
#!/bin/bash
set -e

FEATURE_NAME="$*"

# Prompt for feature name if not provided
if [ -z "$FEATURE_NAME" ]; then
    echo "🌳 Create Git Worktree"
    echo
    read -p "Enter feature description: " FEATURE_NAME

    if [ -z "$FEATURE_NAME" ]; then
        echo "❌ Feature name is required"
        exit 1
    fi
fi

# Convert to safe names
SAFE_NAME=$(echo "$FEATURE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g')
BRANCH_NAME="feature/$SAFE_NAME"
REPO_NAME=$(basename $(git rev-parse --show-toplevel))
WORKTREE_DIR="../${REPO_NAME}-${SAFE_NAME}"

echo
echo "🌳 Creating worktree for: $FEATURE_NAME"
echo "📁 Branch: $BRANCH_NAME"
echo "📂 Directory: $WORKTREE_DIR"
echo

# Confirm before creating
read -p "Continue? [Y/n]: " confirm
if [[ $confirm =~ ^[Nn] ]]; then
    echo "❌ Cancelled"
    exit 0
fi

# Create branch and worktree
echo "Creating branch..."
# Check if branch already exists
if git show-ref --verify --quiet "refs/heads/$BRANCH_NAME"; then
    echo "Branch $BRANCH_NAME already exists, using existing branch..."
    git worktree add "$WORKTREE_DIR" "$BRANCH_NAME"
else
    echo "Creating new branch $BRANCH_NAME from origin/main..."
    # Fetch latest from origin
    git fetch origin main:refs/remotes/origin/main 2>/dev/null || true
    # Create worktree with new branch based on origin/main
    git worktree add -b "$BRANCH_NAME" "$WORKTREE_DIR" origin/main
fi

echo
echo "✅ Worktree created successfully!"
echo "🚀 To switch: cd $WORKTREE_DIR && claude"
echo "📋 Or run: git-tree-switch '$FEATURE_NAME'"
echo "🔧 Or run: git-tree-switch (interactive)"
EOF

# git-tree-list - List worktrees
cat > ~/bin/git-tree-list << 'EOF'
#!/bin/bash
set -e

echo "📋 Git Worktree Status"
echo

# Show current location
echo "📍 Current location:"
echo "   $(pwd)"
echo "   Branch: $(git branch --show-current 2>/dev/null || echo 'Not in git repo')"
echo

# Show all worktrees
echo "🌳 All worktrees:"
git worktree list

echo

# Show recent branches
echo "🌿 Recent branches:"
git branch --sort=-committerdate -a | head -10
EOF

# git-tree-switch - Switch to worktree
cat > ~/bin/git-tree-switch << 'EOF'
#!/bin/bash
set -e

FEATURE_NAME="$*"

# Prompt for feature name if not provided
if [ -z "$FEATURE_NAME" ]; then
    echo "🚀 Switch to Git Worktree"
    echo
    echo "Available worktrees:"
    git worktree list
    echo
    read -p "Enter feature description to switch to: " FEATURE_NAME

    if [ -z "$FEATURE_NAME" ]; then
        echo "❌ Feature name is required"
        exit 1
    fi
fi

# Convert to safe names
SAFE_NAME=$(echo "$FEATURE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-\|-$//g')
REPO_NAME=$(basename $(git rev-parse --show-toplevel))
WORKTREE_DIR="../${REPO_NAME}-${SAFE_NAME}"

if [ ! -d "$WORKTREE_DIR" ]; then
    echo "❌ Worktree not found: $WORKTREE_DIR"
    echo
    echo "🔍 Available worktrees:"
    git worktree list
    echo
    echo "💡 Create it first with: git-tree '$FEATURE_NAME'"
    exit 1
fi

echo "🚀 Switching to: $WORKTREE_DIR"
cd "$WORKTREE_DIR"
exec claude -p "Working on: $FEATURE_NAME"
EOF

# git-tree-merge - Merge back to main
cat > ~/bin/git-tree-merge << 'EOF'
#!/bin/bash
set -e

FEATURE_DESC="$*"

# Get current branch info
BRANCH_NAME=$(git branch --show-current)
CURRENT_DIR=$(basename $(pwd))

# Prompt for description if not provided
if [ -z "$FEATURE_DESC" ]; then
    echo "🔄 Merge Feature Back to Main"
    echo
    echo "📋 Current branch: $BRANCH_NAME"
    echo "📁 Current directory: $CURRENT_DIR"
    echo
    read -p "Enter feature description for commit/PR: " FEATURE_DESC

    if [ -z "$FEATURE_DESC" ]; then
        FEATURE_DESC="Feature implementation from $BRANCH_NAME"
    fi
fi

echo
echo "🔄 Merging feature: $FEATURE_DESC"
echo "📋 Branch: $BRANCH_NAME"
echo

# Show current status
echo "📊 Current status:"
git status --short

# Confirm before proceeding
echo
read -p "Commit, push, and create PR? [Y/n]: " confirm
if [[ $confirm =~ ^[Nn] ]]; then
    echo "❌ Cancelled"
    exit 0
fi

# Commit any changes
if ! git diff-index --quiet HEAD --; then
    echo "📝 Committing changes..."
    git add .
    git commit -m "feat: $FEATURE_DESC"
else
    echo "ℹ️  No changes to commit"
fi

# Push branch
echo "🚀 Pushing branch..."
git push origin "$BRANCH_NAME"

# Create PR if possible
if command -v gh >/dev/null 2>&1; then
    echo "📝 Creating PR..."
    gh pr create --title "feat: $FEATURE_DESC" --body "Implementation of: $FEATURE_DESC" --base main
    echo "✅ PR created!"
else
    echo "💡 Install GitHub CLI (gh) for automatic PR creation"
    echo "📋 Branch pushed: $BRANCH_NAME"
fi

echo
echo "🧹 After PR is merged, run: git-tree-cleanup"
EOF

# git-tree-cleanup - Clean up worktree
cat > ~/bin/git-tree-cleanup << 'EOF'
#!/bin/bash
set -e

CURRENT_DIR=$(pwd)
MAIN_REPO=$(git rev-parse --show-toplevel 2>/dev/null || echo "")

if [ "$CURRENT_DIR" = "$MAIN_REPO" ]; then
    echo "❌ Run this from inside a worktree, not the main repo"
    echo
    echo "🔍 Available worktrees:"
    git worktree list
    exit 1
fi

BRANCH_NAME=$(git branch --show-current)
WORKTREE_NAME=$(basename "$CURRENT_DIR")

echo "🧹 Clean Up Worktree"
echo
echo "📋 Current worktree: $CURRENT_DIR"
echo "🌿 Current branch: $BRANCH_NAME"
echo "📁 Worktree name: $WORKTREE_NAME"
echo

# Confirm cleanup
read -p "Remove this worktree? [y/N]: " confirm
if [[ ! $confirm =~ ^[Yy] ]]; then
    echo "❌ Cancelled - worktree preserved"
    exit 0
fi

# Go to main repo and remove worktree
echo "🧹 Removing worktree..."
cd "$MAIN_REPO"
git worktree remove "$CURRENT_DIR" --force

echo "✅ Worktree removed!"
echo "📁 Back in main repo: $(pwd)"
echo

# Ask about branch cleanup
read -p "Delete the feature branch '$BRANCH_NAME'? [y/N]: " delete_branch
if [[ $delete_branch =~ ^[Yy] ]]; then
    echo "🗑️  Deleting local branch..."
    git branch -d "$BRANCH_NAME" 2>/dev/null || git branch -D "$BRANCH_NAME"

    read -p "Delete remote branch too? [y/N]: " delete_remote
    if [[ $delete_remote =~ ^[Yy] ]]; then
        echo "🗑️  Deleting remote branch..."
        git push origin --delete "$BRANCH_NAME"
    fi
    echo "✅ Branch cleanup complete!"
else
    echo "ℹ️  Branch preserved: $BRANCH_NAME"
    echo "   To delete later: git branch -d $BRANCH_NAME"
    echo "   To delete remote: git push origin --delete $BRANCH_NAME"
fi
EOF

# Make all scripts executable
chmod +x ~/bin/git-tree ~/bin/git-tree-list ~/bin/git-tree-switch ~/bin/git-tree-merge ~/bin/git-tree-cleanup

echo "✅ Installed commands to ~/bin:"
echo "   git-tree         - Create new worktree + feature branch"
echo "   git-tree-list    - List all worktrees and branches"
echo "   git-tree-switch  - Switch to existing worktree"
echo "   git-tree-merge   - Commit, push, create PR"
echo "   git-tree-cleanup - Remove worktree after merge"
echo
echo "Make sure ~/bin is in your PATH:"
echo '   export PATH="$HOME/bin:$PATH"'
