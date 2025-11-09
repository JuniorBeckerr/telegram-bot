import os, asyncio, time, shutil, json, sys, signal, multiprocessing
from telethon import TelegramClient
from multiprocessing import Manager, Process
from core.media_processor import process_media
from data.database import is_processed
from config.settings import (
    API_ID, API_HASH, PHONE, GROUP_NAME,
    SESSION_PATH, DOWNLOAD_PATH,
    MSG_POR_WORKER, NUM_WORKERS, RETRIES
)

LAST_PROCESSED_PATH = os.path.join("storage", "last_processed.json")

# =========================================================
# 📘 Controle de progresso
# =========================================================
def load_last_processed():
    if os.path.exists(LAST_PROCESSED_PATH):
        try:
            with open(LAST_PROCESSED_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_last_processed(data):
    os.makedirs(os.path.dirname(LAST_PROCESSED_PATH), exist_ok=True)
    with open(LAST_PROCESSED_PATH, "w") as f:
        json.dump(data, f, indent=4)

# =========================================================
# 🧹 Sessões e limpeza
# =========================================================
def cleanup_temp_files():
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH, exist_ok=True)
        return

    for f in os.listdir(DOWNLOAD_PATH):
        fpath = os.path.join(DOWNLOAD_PATH, f)
        if os.path.isfile(fpath) and f.startswith("tmp_"):
            try:
                os.remove(fpath)
                print(f"🧹 Limpando arquivo temporário: {f}")
            except Exception as e:
                print(f"⚠️ Falha ao limpar {f}: {e}")

def ensure_sessions():
    base_name = "sessao_base"
    base_path = os.path.join(SESSION_PATH, base_name)
    base_file = base_path + ".session"
    os.makedirs(SESSION_PATH, exist_ok=True)

    if not os.path.exists(base_file):
        print("⚙️ Criando sessão base...")
        client = TelegramClient(base_path, API_ID, API_HASH)
        client.start(phone=PHONE)
        client.disconnect()
        print(f"✅ Sessão base criada em: {base_file}")

    for i in range(NUM_WORKERS):
        target = os.path.join(SESSION_PATH, f"sessao_{i}.session")
        if not os.path.exists(target):
            shutil.copy2(base_file, target)
            print(f"🧩 Sessão clonada: {target}")

# =========================================================
# 🚀 Worker com fila dinâmica
# =========================================================
def worker_dynamic(idx, queue, shared_dict):
    async def job():
        t0 = time.time()
        print(f"[W{idx}] 🚀 Iniciando worker...")

        session_path = os.path.join(SESSION_PATH, f"sessao_{idx}")
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            print(f"[W{idx}] ⚠️ Sessão não autenticada.")
            shared_dict[idx] = 0
            return

        dialogs = await client.get_dialogs()
        chat = next((d.entity for d in dialogs if GROUP_NAME in d.name), None)
        if not chat:
            print(f"[W{idx}] ❌ Grupo não encontrado.")
            shared_dict[idx] = 0
            await client.disconnect()
            return

        stop_flag = {"stop": False}
        signal.signal(signal.SIGINT, lambda s, f: stop_flag.update(stop=True))
        last_processed_id = 0

        while not queue.empty() and not stop_flag["stop"]:
            try:
                msg_id = queue.get_nowait()
            except Exception:
                break

            try:
                msg = await client.get_messages(chat, ids=msg_id)
                if not msg or not msg.media or is_processed(str(msg.id)):
                    continue

                media_id = str(msg.id)
                tmp_path = None
                retries = 0

                while retries < RETRIES:
                    try:
                        downloaded = await msg.download_media(file=DOWNLOAD_PATH)

                        # Gera nome temporário padronizado (com a extensão correta)
                        if downloaded and os.path.exists(downloaded):
                            ext = os.path.splitext(downloaded)[1].lower()
                            tmp_name = f"tmp_{msg.id}_{idx}_{int(time.time())}{ext}"
                            tmp_path = os.path.join(DOWNLOAD_PATH, tmp_name)
                            os.rename(downloaded, tmp_path)
                        else:
                            print(f"[W{idx}] ⚠️ Falha ao baixar {msg.id}")
                            continue

                        if not downloaded or not os.path.exists(tmp_path):
                            retries += 1
                            print(f"[W{idx}] ⚠️ Falha ao baixar {media_id}, tentativa {retries}")
                            continue

                        process_media(tmp_path, media_id, msg.date, idx)
                        last_processed_id = msg.id
                        break
                    except Exception as e:
                        retries += 1
                        print(f"[W{idx}] ⚠️ Erro {e} (tentativa {retries})")
                        if tmp_path and os.path.exists(tmp_path):
                            os.remove(tmp_path)
            except Exception as e:
                print(f"[W{idx}] ⚠️ Falha no ID {msg_id}: {e}")

        await client.disconnect()
        shared_dict[idx] = last_processed_id
        print(f"[W{idx}] ✅ Finalizado em {time.time() - t0:.1f}s (último ID {last_processed_id})")

    asyncio.run(job())

# =========================================================
# 🧠 Pré-scan de mensagens reais (melhorado)
# =========================================================
async def prefetch_valid_ids():
    print("🔍 Pré-buscando mensagens válidas...")
    session_path = os.path.join(SESSION_PATH, "sessao_base")
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.start(phone=PHONE)

    dialogs = await client.get_dialogs()
    chat = next((d.entity for d in dialogs if GROUP_NAME in d.name), None)
    if not chat:
        print("❌ Grupo não encontrado.")
        await client.disconnect()
        sys.exit(1)

    last_data = load_last_processed()
    group_state = last_data.get(GROUP_NAME, {})
    last_id = group_state.get("last_id", 0)

    total_to_fetch = MSG_POR_WORKER * NUM_WORKERS * 2
    print(f"📦 Buscando até {total_to_fetch} mensagens (a partir do ID {last_id})...")

    msgs = []
    async for m in client.iter_messages(chat, limit=total_to_fetch, offset_id=last_id, reverse=True):
        if m.media and not is_processed(str(m.id)):
            msgs.append(m)

    await client.disconnect()
    print(f"✅ {len(msgs)} mensagens válidas encontradas.")
    return [m.id for m in msgs]

# =========================================================
# 🧩 Controlador principal com fila balanceada
# =========================================================
def run_workers():
    ensure_sessions()
    cleanup_temp_files()

    valid_ids = asyncio.run(prefetch_valid_ids())
    if not valid_ids:
        print("⚠️ Nenhuma mensagem nova encontrada. Nada a fazer.")
        return

    print(f"👷 {NUM_WORKERS} worker(s) ativos com fila dinâmica...\n")

    with Manager() as manager:
        shared_dict = manager.dict()
        queue = manager.Queue()

        for vid in valid_ids:
            queue.put(vid)

        processes = []
        for i in range(NUM_WORKERS):
            p = Process(target=worker_dynamic, args=(i, queue, shared_dict))
            p.start()
            processes.append(p)

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            print("\n🟥 Interrupção detectada! Finalizando workers com segurança...")
            for p in processes:
                p.terminate()

        last_processed = max(shared_dict.values()) if shared_dict else 0
        last_data = load_last_processed()
        last_data.setdefault(GROUP_NAME, {})["last_id"] = last_processed
        save_last_processed(last_data)

        cleanup_temp_files()
        print(f"\n💾 Progresso global atualizado até ID {last_processed}")
        print("🏁 Todos os workers finalizados com sucesso.\n")

# =========================================================
if __name__ == "__main__":
    print(f"\n🚀 Iniciando processamento com {NUM_WORKERS} worker(s)...")
    run_workers()
