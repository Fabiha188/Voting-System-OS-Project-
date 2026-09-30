\# 🗳️ Voting System — Synchronized Concurrency-Controlled Election Simulator



> A multi-threaded and multi-process voting system demonstrating POSIX synchronization mechanisms — Readers-Writers problem, shared memory, semaphores, worker thread pool, Named Pipe IPC, and SFML GUI.



!\[C](https://img.shields.io/badge/C-00599C?style=for-the-badge\&logo=c\&logoColor=white)

!\[C++](https://img.shields.io/badge/C++-00599C?style=for-the-badge\&logo=cplusplus\&logoColor=white)

!\[POSIX](https://img.shields.io/badge/POSIX-Threads%20%26%20Semaphores-blue?style=for-the-badge)

!\[SFML](https://img.shields.io/badge/SFML-2.6-8CC445?style=for-the-badge\&logo=sfml\&logoColor=white)

!\[Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge\&logo=linux\&logoColor=black)

!\[Course](https://img.shields.io/badge/CS--2006-Operating%20Systems-purple?style=for-the-badge)



\---



\## 📖 Overview



\*\*Voting System\*\* is the final Operating Systems project for \*\*CS-2006\*\* at \*\*FAST NUCES\*\*. It implements a synchronized, concurrency-controlled election simulator that demonstrates the classic \*\*Readers-Writers problem\*\* using \*\*POSIX named semaphores\*\* and \*\*shared memory\*\*.



Voters act as \*\*writers\*\*, result viewers act as \*\*readers\*\*. The system supports three operational modes — \*\*Manual Voting\*\*, \*\*Auto Thread Mode (pthreads)\*\*, and \*\*Auto Process Mode (fork)\*\* — with strict data integrity, duplicate vote prevention, and real-time performance analysis.



\*\*Team Members:\*\*

\- Iqra Liaquat — 24K-1024

\- Fabiha Siddiqui — 24K-1022

\- Syeda Maha Fatima — 24K-0624



\*\*Course Instructor:\*\* Ubaid Ullah Sir | Section B | CS-4J



\---



\## ✨ Features



| Feature | Description |

|---|---|

| 🧵 \*\*Multi-threading (pthreads)\*\* | Concurrent voter threads in Auto Thread mode |

| ⚙️ \*\*Multi-processing (fork)\*\* | Concurrent voter processes in Auto Process mode |

| 🔒 \*\*Readers-Writers Lock\*\* | Three POSIX named semaphores (`g\_mutex`, `g\_wrt`, `g\_rc`) |

| 💾 \*\*Shared Memory\*\* | `shm\_open` + `mmap` shares `VotingData` across all threads/processes |

| 🚫 \*\*Duplicate Vote Prevention\*\* | Atomic check-and-update inside writer critical section |

| 👷 \*\*Worker Thread Pool\*\* | Fixed pool consuming a shared task queue — reduces per-voter overhead |

| 📡 \*\*Named Pipe (FIFO)\*\* | `/tmp/voting\_admin\_fifo` — external admin control from terminal |

| 🖥️ \*\*SFML GUI\*\* | Console-style interface with real-time live updates |

| 📊 \*\*Performance Recorder\*\* | Threads vs Processes wall-clock timing using `clock\_gettime(CLOCK\_MONOTONIC)` |

| 📝 \*\*Timestamped Logging\*\* | Every event appended to log file with voter ID and candidate |

| 🔐 \*\*Admin Authentication\*\* | Password-protected admin panel |

| 📤 \*\*Election Report Export\*\* | Full results with percentages exported to text file |



\---



\## 🖼️ Screenshots



\### 1. Main Screen

!\[Main Screen](screenshots/01-main-menu.jpeg)

\*Main screen with live voting status, totals, and five navigation options.\*



\### 2. Admin Panel

!\[Admin Panel](screenshots/02-admin-panel.jpeg)

\*Password-protected admin panel with seven control options.\*



\### 3. Live Voter Monitoring

!\[Live Monitoring](screenshots/03-live-monitoring.jpeg)

\*Real-time activity feed showing voters, candidates, and timestamped events.\*



\### 4. Manual Voting Mode

!\[Manual Voting](screenshots/04-manual-voting.jpeg)

\*Manually cast a vote — voter ID entry, city selection, and candidate choice.\*



\### 5. Auto Mode — Running

!\[Auto Mode](screenshots/05-auto-mode.jpeg)

\*Auto voting in progress with live progress bar and elapsed time (thread/process mode).\*



\### 6. Election Results

!\[Results](screenshots/06-results.jpeg)

\*Final results screen with winner card, vote counts, and percentages.\*



\### 7. Performance Comparison

!\[Performance](screenshots/07-performance.jpeg)

\*Thread mode vs Process mode timing — threads run 3–4× faster.\*



\### 8. Named Pipe — Terminal Commands

!\[FIFO Terminal](screenshots/08-fifo-terminal.jpeg)

\*External admin control via `echo` commands sent to `/tmp/voting\_admin\_fifo`.\*



\---



\## 🏗️ System Architecture



The system is built around a \*\*shared memory segment\*\* (`VotingData`) that holds:

\- Candidate names and vote counters

\- Voted voter IDs (for duplicate detection)

\- Activity log

\- Control flags (voting active, results visible)



| Component | Role |

|---|---|

| \*\*Shared Memory (`VotingData`)\*\* | Stores candidates, votes, voter IDs, activity log, control flags |

| \*\*Readers-Writers Locks\*\* | `g\_mutex` (reader count) · `g\_wrt` (exclusive writer) · `g\_rc` (reader count updates) |

| \*\*Voter Threads / Processes\*\* | Each voter casts a single vote concurrently (pthread or fork) |

| \*\*Observer (Result Viewer)\*\* | Multiple readers can view results simultaneously |

| \*\*Worker Thread Pool\*\* | Fixed pool serving a task queue (default: 4 workers) |

| \*\*Named Pipe (FIFO)\*\* | External admin command channel |

| \*\*GUI (SFML 2.6)\*\* | Console-style rendering with live updates |

| \*\*Logging Module\*\* | Timestamped events to text file |

| \*\*Performance Recorder\*\* | Wall-clock timing per auto-voting session |



\---



\## 🔒 Synchronization — Readers-Writers Solution



The classic \*\*Readers-Writers problem\*\* is solved using the \*\*first variant (readers preference)\*\* — appropriate for a voting system where result viewers (readers) are more frequent than voters (writers).



\### Semaphore Initialization



```c

g\_mutex = sem\_open("/voting\_mutex",      O\_CREAT, 0600, 1);

g\_wrt   = sem\_open("/voting\_write",      O\_CREAT, 0600, 1);

g\_rc    = sem\_open("/voting\_read\_count", O\_CREAT, 0600, 1);

