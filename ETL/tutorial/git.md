# 🛠️ Terminal Workflow: LazyGit & GHDash Integration

Tài liệu này ghi lại quy trình làm việc chuẩn **"Terminal Warrior"** để quản lý Pull Request và xử lý xung đột (Conflict) mà không cần rời khỏi Terminal.

---

## 1. ⚙️ Cấu hình LazyGit (Custom Commands)

Dán nội dung này vào file config của LazyGit:

```
~/.config/lazygit/config.yml
```

```yaml
customCommands:
  - key: 'C'
    context: 'localBranches'
    description: 'Tao Pull Request thuan Terminal 100%'
    command: "gh pr create --title '{{.SelectedLocalBranch.Name}}' --body 'Created from Terminal Warrior' && gh pr view --web=false"
    loadingText: 'Dang thinh cau Pull Request (Terminal Only)...'
```

---

## 2. 🔥 Quy trình Xử lý Xung đột (Conflict Resolution) trong LazyGit

Khi GitHub báo:

```
This branch has conflicts
```

Thực hiện các bước sau ngay tại máy:

### 🧩 Checkout & Merge

- Trong LazyGit → nhấn `3` (**Branches**)
- Nhấn `Space` vào nhánh làm việc (ví dụ: `etl`)
- Di chuyển tới nhánh `main`
- Nhấn `M` (`Shift + m`) để merge `main` vào nhánh hiện tại

---

### ✂️ Giải quyết (Pick code)

- Nhấn `2` (**Files**)
- Tìm file có trạng thái `UU` (Unmerged)
- Nhấn `Enter` để vào chế độ resolve conflict
- Dùng phím mũi tên di chuyển
- Nhấn `Space` để chọn đoạn code muốn giữ (Pick)
- Nhấn `Esc` để thoát

---

### 🚀 Commit & Push

- Nhấn `c` để commit bản resolve
- Nhấn `P` (`Shift + p`) để push lên GitHub

---

## 3. ⚔️ Kết liễu Pull Request với GHDash

Sau khi xử lý conflict xong ở local:

### 🖥️ Mở GHDash

```bash
ghdash
```

### 🔄 Refresh

- Nhấn `r` để cập nhật trạng thái  
→ Khung đỏ (Conflict) sẽ biến mất

---

### ✅ Merge PR

- Di chuyển tới Pull Request
- Nhấn `m`
- Chọn:

```
Create a merge commit
```

- Nhấn `Enter` để hoàn tất

---

## 🎯 Tổng kết

Workflow full terminal:

- LazyGit → tạo PR + resolve conflict
- GHDash → quản lý & merge PR

👉 Không cần mở browser. Làm việc như một **Terminal Warrior** thực thụ.
