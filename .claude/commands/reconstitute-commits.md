# Reconstitute Commits

Analyze all commits on the current branch since diverging from the base branch, then help reorganize them into clean, logical commits.

## Instructions

1. First, identify the base branch (default: `main`) and analyze all commits since diverging from it.

2. Run these commands to gather information:
   - `git log --oneline <base>..HEAD` - list all commits
   - `git diff --stat <base>..HEAD` - show files changed
   - `git diff <base>..HEAD` - full diff for analysis

3. Categorize all changes into logical groups:
   - **Bug fixes** - corrections to existing functionality
   - **Features** - new functionality
   - **Refactoring** - code improvements without behavior change
   - **Tests** - new or updated tests
   - **Documentation** - comments, docs, README changes
   - **Chores** - dependencies, config, build changes

4. Present a proposed commit structure like:
   ```
   Proposed commits:
   1. fix: <description>
      - file1.java (lines X-Y)
      - file2.java (lines A-B)

   2. feat: <description>
      - file3.java
      - file4.java
   ```

5. Ask the user to confirm or adjust the groupings.

6. Guide them through the reconstitution process:
   ```bash
   # Save current state
   git branch backup-before-reconstitute

   # Soft reset to base
   git reset --soft <base>

   # Unstage all
   git reset HEAD

   # Then for each logical commit, use:
   git add -p <files>  # or git add <file> for whole files
   git commit -m "<message>"
   ```

7. After reconstitution, remind them to:
   ```bash
   # If already pushed, force push the clean history
   git push --force-with-lease origin <branch>

   # Delete backup branch when satisfied
   git branch -d backup-before-reconstitute
   ```

## Arguments

$ARGUMENTS - Reference point for commits to analyze. Can be:
- Branch name: `main`, `develop`
- Commit SHA: `abc123`
- Relative: `HEAD~5` (last 5 commits)
- Date: `--since="2024-12-13"` or `--since="3 days ago"`

Defaults to "main" if not specified.

## Example Usage

```
/reconstitute-commits
/reconstitute-commits develop
/reconstitute-commits HEAD~10
/reconstitute-commits abc123f
/reconstitute-commits --since="2024-12-13"
/reconstitute-commits --since="yesterday"
```

## Date-Based Workflow

When using `--since`, the command will:
1. Find all commits since that date
2. Identify the parent of the earliest commit as the reset point
3. Proceed with normal reconstitution
