"""Manage admin users from the command line.

The container WORKDIR is /app and the package lives at /app/backend/app,
so the import path is ``backend.app.scripts.admin_user``.

Examples:

  # List all admin accounts:
  docker compose exec -T web python -m backend.app.scripts.admin_user list

  # Show one user (any email, admin or not):
  docker compose exec -T web python -m backend.app.scripts.admin_user show user@example.com

  # Promote an existing user to admin:
  docker compose exec -T web python -m backend.app.scripts.admin_user grant user@example.com

  # Revoke admin rights:
  docker compose exec -T web python -m backend.app.scripts.admin_user revoke user@example.com

  # Reset password (read from --password or interactively if missing):
  docker compose exec -T web python -m backend.app.scripts.admin_user set-password user@example.com --password "NewS3cret!"

  # Create a fresh admin in one shot (use this when there is no admin yet):
  docker compose exec -T web python -m backend.app.scripts.admin_user create user@example.com --password "Init12345" --name "Owner"
"""

from __future__ import annotations

import argparse
import getpass
import sys
from typing import Optional

from sqlalchemy import select

from ..db import SessionLocal
from ..models import User
from ..services.auth_service import AuthService


def _print_user(user: User) -> None:
    print(f"id={user.id}")
    print(f"email={user.email}")
    print(f"name={user.full_name or '-'}")
    print(f"phone={user.phone or '-'}")
    print(f"is_admin={user.is_admin}")
    print(f"is_active={user.is_active}")
    print(f"email_verified_at={user.email_verified_at or '-'}")
    print(f"phone_verified_at={user.phone_verified_at or '-'}")
    print(f"created_at={user.created_at}")
    print(f"last_login_at={user.last_login_at or '-'}")


def _resolve_user(db, email: str) -> Optional[User]:
    return db.execute(
        select(User).where(User.email == email.strip().lower())
    ).scalar_one_or_none()


def _read_password_from_args(args: argparse.Namespace) -> str:
    if args.password:
        return args.password
    pw = getpass.getpass("Новый пароль: ")
    pw2 = getpass.getpass("Повторите: ")
    if pw != pw2:
        sys.exit("Пароли не совпадают")
    if len(pw) < 8:
        sys.exit("Пароль должен быть не короче 8 символов")
    return pw


def cmd_list(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        rows = db.execute(
            select(User).where(User.is_admin.is_(True)).order_by(User.id.asc())
        ).scalars().all()
        if not rows:
            print("Админов в БД нет.")
            return
        print(f"{'id':>4}  {'email':32}  {'имя':24}  {'верифицирован':14}")
        for u in rows:
            verified = "email" if u.email_verified_at else ""
            if u.phone_verified_at:
                verified = (verified + ",phone" if verified else "phone")
            print(f"{u.id:>4}  {u.email:32}  {(u.full_name or '-'):24}  {verified or '-':14}")


def cmd_show(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        user = _resolve_user(db, args.email)
        if not user:
            sys.exit(f"Пользователь {args.email} не найден")
        _print_user(user)


def cmd_grant(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        user = _resolve_user(db, args.email)
        if not user:
            sys.exit(f"Пользователь {args.email} не найден")
        if user.is_admin:
            print(f"{user.email} уже админ.")
            return
        user.is_admin = True
        db.commit()
        print(f"OK — {user.email} теперь администратор.")


def cmd_revoke(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        user = _resolve_user(db, args.email)
        if not user:
            sys.exit(f"Пользователь {args.email} не найден")
        if not user.is_admin:
            print(f"{user.email} не был админом.")
            return
        user.is_admin = False
        db.commit()
        print(f"OK — права админа сняты у {user.email}.")


def cmd_set_password(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        user = _resolve_user(db, args.email)
        if not user:
            sys.exit(f"Пользователь {args.email} не найден")
        password = _read_password_from_args(args)
        user.password_hash = AuthService(db)._hash(password)
        db.commit()
        print(f"OK — пароль для {user.email} обновлён.")


def cmd_create(args: argparse.Namespace) -> None:
    with SessionLocal() as db:
        existing = _resolve_user(db, args.email)
        if existing:
            sys.exit(f"Пользователь {args.email} уже существует. Используйте grant / set-password.")
        password = _read_password_from_args(args)
        auth = AuthService(db)
        user = auth.create_user(
            email=args.email,
            password=password,
            full_name=args.name or None,
            is_admin=True,
        )
        print(f"OK — создан администратор {user.email} (id={user.id}).")


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Управление администраторами Level Avto")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="Показать список админов").set_defaults(func=cmd_list)

    sp = sub.add_parser("show", help="Показать одну запись")
    sp.add_argument("email")
    sp.set_defaults(func=cmd_show)

    sp = sub.add_parser("grant", help="Сделать пользователя админом")
    sp.add_argument("email")
    sp.set_defaults(func=cmd_grant)

    sp = sub.add_parser("revoke", help="Снять права админа")
    sp.add_argument("email")
    sp.set_defaults(func=cmd_revoke)

    sp = sub.add_parser("set-password", help="Установить новый пароль")
    sp.add_argument("email")
    sp.add_argument("--password", help="Если не указан — будет запрошен интерактивно")
    sp.set_defaults(func=cmd_set_password)

    sp = sub.add_parser("create", help="Создать нового админа")
    sp.add_argument("email")
    sp.add_argument("--password", help="Если не указан — будет запрошен интерактивно")
    sp.add_argument("--name", default=None, help="Полное имя")
    sp.set_defaults(func=cmd_create)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
