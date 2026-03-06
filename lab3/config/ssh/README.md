# SSH-ключи для GreenPlum кластера

Сгенерируйте SSH-ключи перед запуском:

```bash
ssh-keygen -t rsa -b 3072 -f id_rsa -N "" -C "gpadmin"
cp id_rsa.pub authorized_keys
```

В этой папке должны быть 3 файла:
- `id_rsa` -- приватный ключ (для master)
- `id_rsa.pub` -- публичный ключ (для master)
- `authorized_keys` -- копия публичного ключа (для segments)
