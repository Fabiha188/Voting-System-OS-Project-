#Command to run gcc voting_system.c -o voting -lcsfml-graphics -lcsfml-window -lcsfml-system -lpthread -lm -lrt
#./voting
    


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <math.h>
#include <ctype.h>
#include <sys/select.h>
#include <sys/types.h>
#include <SFML/Graphics.h>
#include <SFML/Window.h>
#include <SFML/System.h>

/* ==================== CONSTANTS ==================== */
#define MAX_CANDIDATES    10
#define MAX_VOTERS        100
#define MAX_NAME_LENGTH   50
#define MAX_ACTIVITY_LOG  500
#define SHM_NAME          "/voting_system_shm"
#define SEM_MUTEX         "/voting_mutex"
#define SEM_WRIT          "/voting_write"
#define SEM_READ_COUNT    "/voting_read_count"
#define SEM_CONSOLE       "/voting_console"
#define SEM_LOG           "/voting_log_sem"
#define ADMIN_PASSWORD    "admin123"
#define FONT_PATH "/usr/share/fonts/truetype/roboto/unhinted/RobotoTTF/Roboto-Regular.ttf"
#define BASE_W            1280
#define BASE_H            720

/* NEW: Named pipe path */
#define FIFO_PATH         "/tmp/voting_admin_fifo"

/* NEW: Task queue size */
#define TASK_QUEUE_SIZE   MAX_VOTERS

/* ==================== SHARED MEMORY STRUCTURE ==================== */
typedef struct {
    int    candidate_count;
    char   candidate_names[MAX_CANDIDATES][MAX_NAME_LENGTH];
    int    votes[MAX_CANDIDATES];
    int    total_votes;
    int    voted_ids[MAX_VOTERS];
    int    voted_count;
    int    reader_count;
    int    voters_completed;
    int    total_voters;
    char   last_activity[MAX_ACTIVITY_LOG][150];
    int    activity_count;
    int    activity_index;
    time_t start_time;
    int    voting_active;
    int    results_visible;
    int    shm_initialized;
} VotingData;

/* ==================== SCREEN IDs ==================== */
typedef enum {
    SCR_SPLASH = -1,
    SCR_MAIN = 0,
    SCR_ADMIN_LOGIN,
    SCR_ADMIN_PANEL,
    SCR_ADMIN_CANDIDATES,
    SCR_ADMIN_ADD_CAND,
    SCR_ADMIN_LIVE,
    SCR_ADMIN_STATS,
    SCR_ADMIN_RESET_CONFIRM,
    SCR_ADMIN_DECLARE_CONFIRM,
    SCR_ADMIN_EXPORT_CONFIRM,
    SCR_ADMIN_PASSWORD_CONFIRM,
    SCR_MANUAL_VOTER_ID,
    SCR_MANUAL_VOTE,
    SCR_MANUAL_SUCCESS,
    SCR_AUTO_SETUP_CITIES,
    SCR_AUTO_VOTER_COUNT,
    SCR_AUTO_VOTER_DETAIL,
    SCR_AUTO_RUNNING,
    SCR_RESULTS,
    SCR_PERF,
    SCR_MSG,
} Screen;

/* ==================== CUSTOM COLOR PALETTE ==================== */
/* ==================== REDESIGNED COLOR PALETTE ==================== */
/* ==================== ELITE COLOR PALETTE ==================== */
/* ==================== LIGHT THEME – C0E1D2, E5EEE4, F6F4E8 ==================== */
/* ==================== DARK THEME – 16476A, 132440, WHITE ==================== */
/* ==================== SOFT PASTEL / MINIMAL THEME (Light & Clean) ==================== */
/* ==================== CUSTOM DARK THEME (Blue gradients + Beige accents) ==================== */
/* ==================== CUSTOM DARK THEME + D2C1B6 ACCENT ==================== */
static sfColor COL(sfUint8 r,sfUint8 g,sfUint8 b,sfUint8 a){sfColor c;c.r=r;c.g=g;c.b=b;c.a=a;return c;}

// Dark backgrounds
static sfColor C_BG      = {27,  60,  83,  255};  // 1B3C53
static sfColor C_PANEL   = {35,  76, 106, 255};   // 234C6A
static sfColor C_CARD    = {69, 104, 130, 255};   // 456882

// Accent – D2C1B6 (beige)
static sfColor C_ACCENT  = {210, 193, 182, 255};
static sfColor C_ACCENT2 = {225, 210, 200, 255};
static sfColor C_WARNING = {210, 193, 182, 255};
static sfColor C_SUCCESS = {56,  142, 60,  255};   // Green rakhna hai to rahega warna beige kar dena

// Text – White
static sfColor C_WHITE   = {255, 255, 255, 255};
static sfColor C_BLACK   = {20,  30,  40,  255};
static sfColor C_LGRAY   = {200, 200, 210, 255};
static sfColor C_DGRAY   = {150, 155, 165, 255};

// Border & selection
static sfColor C_SEL     = {210, 193, 182, 255};
static sfColor C_BORDER  = {90, 115, 140, 255};
static sfColor C_GOLD    = {255, 215, 0,   255};   // Golden for winner

// Shadows & overlays
static sfColor C_SHADOW  = {0,   0,   0,   180};
static sfColor C_HEADER  = {27,  60,  83,  255};
static sfColor C_BTN_HOV = {210, 193, 182, 50};    // Translucent beige hover
static sfColor C_GLASS   = {255, 255, 255, 15};
static sfColor PULSE_COLOR = {210, 193, 182, 255}; // Blinking bar beige
static sfColor C_TRANS   = {0,   0,   0,   0};
/* ==================== GLOBAL BACKEND ==================== */
static sem_t      *g_mutex   = NULL;
static sem_t      *g_wrt     = NULL;
static sem_t      *g_rc      = NULL;
static sem_t      *g_con     = NULL;
static sem_t      *g_log     = NULL;
static VotingData *vd        = NULL;
static int         shm_fd    = -1;
static FILE       *log_file  = NULL;
static char        log_fname[128];
static volatile sig_atomic_t cleanup_flag = 0;

/* ==================== SFML GLOBALS ==================== */
static sfRenderWindow *win = NULL;
static sfFont         *fnt = NULL;
static sfClock        *clk = NULL;
static float           dt  = 0.f;
static float           splash_timer = 0.f;
static int             splash_waiting_for_key = 0;

/* ==================== UI STATE ==================== */
static Screen  cur_scr      = SCR_SPLASH;
static Screen  confirm_yes  = SCR_MAIN;
static Screen  confirm_no   = SCR_ADMIN_PANEL;
static char    confirm_title[128];
static char    confirm_body[256];
static char    msg_text[512];
static Screen  msg_ret;
static float   msg_timer    = 0.f;
static int     confirm_selection = 0;
static char    pw_buf[64];
static int     pw_len       = 0;
static int     admin_tries  = 3;
static char    pw_err[128];
static int     pending_action = 0;
static int     adm_sel      = 0;
#define ADM_ITEMS 7
static int     cand_sel     = 0;
static int     cand_menu_sel = 0;
static int     add_cand_sel = 0;
static int     results_back_sel = 0;
static int     manual_success_sel = 0;
static int     auto_complete_sel = 0;
static char    new_cand[MAX_NAME_LENGTH];
static int     new_cand_len = 0;
static int     del_idx      = -1;
static char    vid_buf[16];
static int     vid_len      = 0;
static int     vid_val      = 0;
static char    vid_err[128];
static int     vote_sel     = 0;
static int     voted_candidate = 0;
static char    vote_err[128];
static int     confirm_vote_show = 0;
static char    manual_city[50];
static int     manual_city_len = 0;
static int     manual_field_sel = 0;
static char    city1[50];
static int     city1_len    = 0;
static char    city2[50];
static int     city2_len    = 0;
static char    nv_buf[8];
static int     nv_len       = 0;
static int     auto_n       = 0;
static int     auto_field   = 0;
static char    auto_setup_err[128];
static int     auto_use_threads = 0;
static int     auto_cur     = 0;
static char    avid_buf[16];
static int     avid_len     = 0;
static char    avid_err[128];
static int     acand_sel    = 0;
typedef struct { int vid; int cid; } AEntry;
static AEntry  auto_entries[MAX_VOTERS];
static int     auto_done_flag   = 0;
static double  auto_elapsed     = 0.0;
static pthread_t auto_tids[MAX_VOTERS];
static struct timespec auto_ts_start;
typedef struct { int vid; int cid; } ThreadArg;
static char    perf_lines[4096];
static sfClock *pulseClock = NULL;

/* ==================== NEW: ROUND-ROBIN WORKER POOL ==================== */
typedef struct {
    int vid;
    int cid;
} VoteTask;

static VoteTask task_queue[TASK_QUEUE_SIZE];
static int task_front = 0;
static int task_rear = 0;
static int task_count = 0;
static pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t task_cond = PTHREAD_COND_INITIALIZER;
static pthread_t *worker_threads = NULL;
static int worker_pool_size = 4;          /* default 4 workers */
static int use_worker_pool = 1;           /* 1 = worker pool enabled by default */
static int tasks_completed = 0;            /* counter for auto mode when using pool */
static int total_tasks_expected = 0;
static pthread_mutex_t pool_stats_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ==================== NEW: NAMED PIPE (FIFO) ==================== */
static int fifo_fd = -1;
static pthread_t fifo_thread;
static int fifo_running = 1;
static void *fifo_listener(void *arg);

/* ==================== FORWARD DECLARATIONS ==================== */
void  backend_init(void);
void  backend_cleanup(int full);
void  reader_enter(void);
void  reader_exit(void);
void  writer_enter(void);
void  writer_exit(void);
int   validate_vid(int id);
int   cast_vote(int vid, int cid);
void  add_activity(const char *s);
void  safe_log(const char *fmt, ...);
void  create_log_file(const char *mode);
void  init_perf_file(void);
void  save_perf(const char *mode,int n,double e,int thr);
void  do_export_report(void);
void  handle_signal(int sig);
int   is_duplicate_candidate(const char *name);
void  log_manual_city_safe(int vid, const char *city);

/* NEW: Worker pool functions */
void init_worker_pool(void);
void destroy_worker_pool(void);
void push_vote_task(int vid, int cid);
void *worker_thread_func(void *arg);
void set_worker_pool_size(int new_size);

/* NEW: Named pipe functions */
void init_fifo(void);
void shutdown_fifo(void);
void process_admin_command(const char *cmd);

/* sfml helpers */
float sx(void);
float sy(void);
float sp(float v);
void  draw_rect(float x,float y,float w,float h,sfColor fill,sfColor out,float ot);
void  draw_text(const char *s,float x,float y,unsigned sz,sfColor c,int cen_x,float cxv);
void  draw_btn(const char *lbl,float x,float y,float w,float h,sfColor bg,sfColor fg,int sel);
void  draw_input(const char *lbl,const char *val,float x,float y,float w,int active,int masked);
void  draw_header(const char *title,const char *sub);
void  draw_footer(const char *hint);
void  draw_progress_bar(float x,float y,float w,float h,float pct,sfColor fg);
void  text_input(sfEvent *e,char *buf,int *len,int maxlen,int dig_only);
void  draw_splash(void);
static sfColor getPulseColor(void) {
    if (!pulseClock) return COL(155, 180, 192, 255); // #9BB4C0
    float seconds = sfTime_asSeconds(sfClock_getElapsedTime(pulseClock));
    int alpha = 80 + (int)(175 * (sin(seconds * 5.0f) + 1.0f) / 2.0f);
    if (alpha > 255) alpha = 255;
    if (alpha < 80) alpha = 80;
    return COL(155, 180, 192, alpha); // #9BB4C0
}

void  go(Screen s);
void  go_msg(const char *txt,Screen ret,float autod);
void  go_confirm(const char *title,const char *body,Screen yes,Screen no);
void  go_password_confirm(int action);

void render(void);
void render_main(void);
void render_admin_login(void);
void render_admin_panel(void);
void render_admin_candidates(void);
void render_admin_add_cand(void);
void render_admin_live(void);
void render_admin_stats(void);
void render_confirm(void);
void render_password_confirm(void);
void render_manual_vid(void);
void render_manual_vote(void);
void render_manual_success(void);
void render_auto_setup_cities(void);
void render_auto_voter_count(void);
void render_auto_voter_detail(void);
void render_auto_running(void);
void render_results(void);
void render_perf(void);
void render_msg(void);

void events(sfEvent *e);
void ev_splash(sfEvent *e);
void ev_main(sfEvent *e);
void ev_admin_login(sfEvent *e);
void ev_admin_panel(sfEvent *e);
void ev_admin_candidates(sfEvent *e);
void ev_admin_add_cand(sfEvent *e);
void ev_admin_live(sfEvent *e);
void ev_admin_stats(sfEvent *e);
void ev_confirm(sfEvent *e);
void ev_password_confirm(sfEvent *e);
void ev_manual_vid(sfEvent *e);
void ev_manual_vote(sfEvent *e);
void ev_auto_setup_cities(sfEvent *e);
void ev_auto_voter_count(sfEvent *e);
void ev_auto_voter_detail(sfEvent *e);
void ev_auto_running(sfEvent *e);
void ev_results(sfEvent *e);
void ev_perf(sfEvent *e);
void ev_msg(sfEvent *e);

void *auto_voter_fn(void *arg);
void auto_start(void);
void auto_update(void);

/* ==================== SIGNAL HANDLER ==================== */
void handle_signal(int sig) { (void)sig; cleanup_flag = 1; }

/* ==================== BACKEND INIT / CLEANUP ==================== */
void backend_init(void) {
    shm_fd = shm_open(SHM_NAME, O_CREAT|O_EXCL|O_RDWR, 0600);
    if (shm_fd == -1 && errno == EEXIST) {
        shm_fd = shm_open(SHM_NAME, O_RDWR, 0600);
        if (shm_fd != -1) {
            vd = (VotingData*)mmap(NULL,sizeof(VotingData),
                                   PROT_READ|PROT_WRITE,MAP_SHARED,shm_fd,0);
            if (vd != MAP_FAILED && vd->shm_initialized == 1) {
                g_mutex = sem_open(SEM_MUTEX,0);
                g_wrt   = sem_open(SEM_WRIT,0);
                g_rc    = sem_open(SEM_READ_COUNT,0);
                g_con   = sem_open(SEM_CONSOLE,0);
                g_log   = sem_open(SEM_LOG,0);
                if (g_mutex!=SEM_FAILED && g_wrt!=SEM_FAILED &&
                    g_rc!=SEM_FAILED && g_con!=SEM_FAILED && g_log!=SEM_FAILED) {
                    signal(SIGINT,handle_signal);
                    signal(SIGTERM,handle_signal);
                    return;
                }
            }
        }
        sem_unlink(SEM_MUTEX); sem_unlink(SEM_WRIT);
        sem_unlink(SEM_READ_COUNT); sem_unlink(SEM_CONSOLE); sem_unlink(SEM_LOG);
        shm_unlink(SHM_NAME);
        shm_fd = shm_open(SHM_NAME, O_CREAT|O_RDWR, 0600);
    }
    if (shm_fd == -1) { fprintf(stderr,"SHM failed\n"); exit(1); }
    if (ftruncate(shm_fd, sizeof(VotingData)) == -1) { fprintf(stderr,"ftruncate failed\n"); exit(1); }

    g_mutex = sem_open(SEM_MUTEX,O_CREAT,0600,1);
    g_wrt   = sem_open(SEM_WRIT,O_CREAT,0600,1);
    g_rc    = sem_open(SEM_READ_COUNT,O_CREAT,0600,1);
    g_con   = sem_open(SEM_CONSOLE,O_CREAT,0600,1);
    g_log   = sem_open(SEM_LOG,O_CREAT,0600,1);
    if (g_mutex==SEM_FAILED||g_wrt==SEM_FAILED||g_rc==SEM_FAILED||
        g_con==SEM_FAILED||g_log==SEM_FAILED) {
        fprintf(stderr,"Semaphore init failed\n"); exit(1);
    }
    vd = (VotingData*)mmap(NULL,sizeof(VotingData),
                           PROT_READ|PROT_WRITE,MAP_SHARED,shm_fd,0);
    if (vd == MAP_FAILED) { fprintf(stderr,"mmap failed\n"); exit(1); }
    memset(vd,0,sizeof(VotingData));
    vd->voting_active    = 1;
    vd->results_visible  = 0;
    vd->shm_initialized  = 1;
    vd->activity_index   = 0;
    vd->candidate_count  = 0;
   
    signal(SIGINT,handle_signal);
    signal(SIGTERM,handle_signal);
}

void backend_cleanup(int full) {
    if (log_file && vd) {
        time_t now; time(&now);
        safe_log("\nSession ended: %s",ctime(&now));
        safe_log("Total votes: %d\n", vd->total_votes);
        fclose(log_file); log_file = NULL;
    }
    if (g_mutex) sem_close(g_mutex);
    if (g_wrt)   sem_close(g_wrt);
    if (g_rc)    sem_close(g_rc);
    if (g_con)   sem_close(g_con);
    if (g_log)   sem_close(g_log);
    if (full) {
        sem_unlink(SEM_MUTEX); sem_unlink(SEM_WRIT);
        sem_unlink(SEM_READ_COUNT); sem_unlink(SEM_CONSOLE); sem_unlink(SEM_LOG);
    }
    if (vd && vd != MAP_FAILED) munmap(vd, sizeof(VotingData));
    if (shm_fd != -1) { close(shm_fd); if (full) shm_unlink(SHM_NAME); }
}

void cleanup_on_exit(void) {
    destroy_worker_pool();
    shutdown_fifo();
    backend_cleanup(1);
}

/* ==================== READER / WRITER LOCKS ==================== */
void reader_enter(void) {
    sem_wait(g_rc); vd->reader_count++;
    if (vd->reader_count==1) sem_wait(g_wrt);
    sem_post(g_rc);
}
void reader_exit(void) {
    sem_wait(g_rc); vd->reader_count--;
    if (vd->reader_count==0) sem_post(g_wrt);
    sem_post(g_rc);
}
void writer_enter(void) { sem_wait(g_wrt); }
void writer_exit(void)  { sem_post(g_wrt); }

/* ==================== VOTE LOGIC ==================== */
int validate_vid(int id) { return (id >= 1000 && id <= 9999); }

void add_activity(const char *s) {
    int idx = vd->activity_count % MAX_ACTIVITY_LOG;
    strncpy(vd->last_activity[idx], s, 149);
    vd->last_activity[idx][149] = '\0';
    vd->activity_count++;
}

void safe_log(const char *fmt, ...) {
    if (!log_file || !g_log) return;
    sem_wait(g_log);
    va_list a; va_start(a,fmt);
    vfprintf(log_file,fmt,a);
    va_end(a); fflush(log_file);
    sem_post(g_log);
}

void create_log_file(const char *mode) {
    time_t now; char ts[32]; time(&now);
    strftime(ts,sizeof(ts),"%d-%m-%Y_%H-%M-%S",localtime(&now));
    snprintf(log_fname,sizeof(log_fname),"vote_log_[%s]_%s.txt",ts,mode);
    log_file = fopen(log_fname,"w");
    if (!log_file) return;
    safe_log("=================================================\n");
    safe_log("VOTING SESSION LOG — %s MODE\n",mode);
    safe_log("Session started: %s",ctime(&now));
    safe_log("-------------------------------------------------\n\n");
}

void init_perf_file(void) {
    FILE *pf = fopen("performance_data.txt","r");
    if (pf) { fclose(pf); return; }
    pf = fopen("performance_data.txt","w");
    if (!pf) return;
    time_t now; time(&now);
    fprintf(pf,"VOTING SYSTEM PERFORMANCE DATA\nCreated: %s\n\n",ctime(&now));
    fclose(pf);
}

void save_perf(const char *mode,int n,double e,int thr) {
    FILE *pf = fopen("performance_data.txt","a");
    if (!pf) return;
    time_t now; char ts[32]; time(&now);
    strftime(ts,sizeof(ts),"[%d-%m-%Y %H:%M:%S]",localtime(&now));
    fprintf(pf,"%s %s (%s): %d voters, %.4fs, %.4f s/voter\n",
            ts,mode,thr?"Thread":"Process",n,e,n>0?e/n:0.0);
    fclose(pf);
}

int is_duplicate_candidate(const char *name) {
    for (int i = 0; i < vd->candidate_count; i++) {
        if (strcasecmp(vd->candidate_names[i], name) == 0) {
            return 1;
        }
    }
    return 0;
}

void log_manual_city_safe(int vid, const char *city) {
    if (!city || strlen(city) == 0) return;
    writer_enter();
    char act[150];
    time_t now;
    char ts[32];
    time(&now);
    strftime(ts,sizeof(ts),"%H:%M:%S",localtime(&now));
    snprintf(act, sizeof(act), "[%s] Voter %d from city '%s' voted", ts, vid, city);
    add_activity(act);
    safe_log("%s\n", act);
    writer_exit();
}

int cast_vote(int vid, int cid) {
    char act[150]; time_t now; char ts[32];

    writer_enter();

    if (!vd->voting_active) { writer_exit(); return -1; }
    if (!validate_vid(vid)) { writer_exit(); return -2; }
    for (int i=0;i<vd->voted_count;i++)
        if (vd->voted_ids[i]==vid) { writer_exit(); return -3; }
    if (cid<0||cid>=vd->candidate_count) { writer_exit(); return -4; }
    if (vd->voted_count>=MAX_VOTERS) { writer_exit(); return -5; }

    vd->votes[cid]++;
    vd->total_votes++;
    vd->voted_ids[vd->voted_count++] = vid;

    time(&now); strftime(ts,sizeof(ts),"%H:%M:%S",localtime(&now));
    snprintf(act,sizeof(act),"[%s] Voter %d voted for %s",
             ts,vid,vd->candidate_names[cid]);
    add_activity(act);
    writer_exit();

    safe_log("%s\n",act);

    sem_wait(g_mutex); vd->voters_completed++; sem_post(g_mutex);
    return 1;
}

void do_export_report(void) {
    time_t now; char ts[32]; time(&now);
    strftime(ts,sizeof(ts),"%d-%m-%Y_%H-%M-%S",localtime(&now));
    char fname[128]; snprintf(fname,sizeof(fname),"election_report_%s.txt",ts);
    FILE *fp = fopen(fname,"w");
    if (!fp) return;
    reader_enter();
    fprintf(fp,"========================================\nELECTION REPORT — %s\n========================================\n\n",ts);
    fprintf(fp,"TOTAL VOTES: %d\nUNIQUE VOTERS: %d\n\nRESULTS:\n",vd->total_votes,vd->voted_count);
    for (int i=0;i<vd->candidate_count;i++) {
        float pct = vd->total_votes>0?(float)vd->votes[i]/vd->total_votes*100.f:0.f;
        fprintf(fp,"%d. %s: %d votes (%.1f%%)\n",i+1,vd->candidate_names[i],vd->votes[i],pct);
    }
    fprintf(fp,"\nACTIVITY LOG:\n");
    for (int i=0;i<vd->activity_count;i++)
        fprintf(fp,"%s\n",vd->last_activity[i%MAX_ACTIVITY_LOG]);
    fprintf(fp,"\n========================================\n");
    reader_exit();
    fclose(fp);
    snprintf(msg_text,sizeof(msg_text),"Report saved:\n%s",fname);
}

/* ==================== ROUND-ROBIN WORKER POOL ==================== */
void *worker_thread_func(void *arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&task_mutex);
        while (task_count == 0 && fifo_running && !cleanup_flag) {
            pthread_cond_wait(&task_cond, &task_mutex);
        }
        if (!fifo_running && task_count == 0) {
            pthread_mutex_unlock(&task_mutex);
            break;
        }
        VoteTask t = task_queue[task_front];
        task_front = (task_front + 1) % TASK_QUEUE_SIZE;
        task_count--;
        pthread_mutex_unlock(&task_mutex);

        /* Actually cast the vote */
        cast_vote(t.vid, t.cid);

        pthread_mutex_lock(&pool_stats_mutex);
        tasks_completed++;
        pthread_mutex_unlock(&pool_stats_mutex);
    }
    return NULL;
}

void init_worker_pool(void) {
    if (worker_threads) return;
    worker_threads = malloc(sizeof(pthread_t) * worker_pool_size);
    for (int i = 0; i < worker_pool_size; i++) {
        pthread_create(&worker_threads[i], NULL, worker_thread_func, NULL);
    }
    use_worker_pool = 1;
    char act[100];
    snprintf(act, sizeof(act), "Worker pool enabled with %d threads", worker_pool_size);
    add_activity(act);
    safe_log("%s\n", act);
}

void destroy_worker_pool(void) {
    if (!worker_threads) return;
    use_worker_pool = 0;
    /* Wake all workers so they exit */
    pthread_mutex_lock(&task_mutex);
    pthread_cond_broadcast(&task_cond);
    pthread_mutex_unlock(&task_mutex);
    for (int i = 0; i < worker_pool_size; i++) {
        pthread_join(worker_threads[i], NULL);
    }
    free(worker_threads);
    worker_threads = NULL;
}

void push_vote_task(int vid, int cid) {
    pthread_mutex_lock(&task_mutex);
    if (task_count < TASK_QUEUE_SIZE) {
        task_queue[task_rear].vid = vid;
        task_queue[task_rear].cid = cid;
        task_rear = (task_rear + 1) % TASK_QUEUE_SIZE;
        task_count++;
        pthread_cond_signal(&task_cond);
    } else {
        /* Queue full – fallback to direct vote (should not happen) */
        cast_vote(vid, cid);
    }
    pthread_mutex_unlock(&task_mutex);
}

void set_worker_pool_size(int new_size) {
    if (new_size < 1) new_size = 1;
    if (new_size > 32) new_size = 32;
    if (worker_threads) {
        /* Recreate pool with new size */
        destroy_worker_pool();
        worker_pool_size = new_size;
        init_worker_pool();
    } else {
        worker_pool_size = new_size;
    }
    char act[80];
    snprintf(act, sizeof(act), "Worker pool size set to %d", worker_pool_size);
    add_activity(act);
    safe_log("%s\n", act);
}

/* ==================== NAMED PIPE (FIFO) ==================== */
void init_fifo(void) {
    unlink(FIFO_PATH);
    if (mkfifo(FIFO_PATH, 0666) == -1 && errno != EEXIST) {
        perror("mkfifo");
        return;
    }
    fifo_fd = open(FIFO_PATH, O_RDWR);
    if (fifo_fd == -1) {
        perror("open fifo");
        return;
    }
    pthread_create(&fifo_thread, NULL, fifo_listener, NULL);
    safe_log("Admin FIFO created at %s\n", FIFO_PATH);
    add_activity("Admin FIFO ready");
}

void shutdown_fifo(void) {
    fifo_running = 0;
    if (fifo_fd != -1) {
        close(fifo_fd);
        fifo_fd = -1;
    }
    unlink(FIFO_PATH);
    pthread_join(fifo_thread, NULL);
}

void process_admin_command(const char *cmd) {
    char buf[256];
    strncpy(buf, cmd, sizeof(buf)-1);
    buf[sizeof(buf)-1] = '\0';
    /* Remove newline */
    char *nl = strchr(buf, '\n');
    if (nl) *nl = '\0';

    if (strcmp(buf, "pause") == 0) {
        writer_enter();
        vd->voting_active = 0;
        writer_exit();
        add_activity("Admin paused voting");
        safe_log("Admin command: pause\n");
    }
    else if (strcmp(buf, "resume") == 0) {
        writer_enter();
        vd->voting_active = 1;
        writer_exit();
        add_activity("Admin resumed voting");
        safe_log("Admin command: resume\n");
    }
    else if (strcmp(buf, "status") == 0) {
        char st[256];
        reader_enter();
        snprintf(st, sizeof(st), "Status: voting=%s, results=%s, total_votes=%d",
                 vd->voting_active?"ACTIVE":"ENDED",
                 vd->results_visible?"VISIBLE":"HIDDEN",
                 vd->total_votes);
        reader_exit();
        add_activity(st);
        safe_log("Admin status request: %s\n", st);
    }
    else if (strcmp(buf, "declare_results") == 0) {
        writer_enter();
        vd->voting_active = 0;
        vd->results_visible = 1;
        writer_exit();
        add_activity("Admin declared results (voting ended)");
        safe_log("Admin command: declare_results\n");
    }
    else if (strcmp(buf, "reset") == 0) {
        writer_enter();
        memset(vd->votes, 0, sizeof(vd->votes));
        memset(vd->voted_ids, 0, sizeof(vd->voted_ids));
        vd->total_votes = 0;
        vd->voted_count = 0;
        vd->voters_completed = 0;
        memset(vd->last_activity, 0, sizeof(vd->last_activity));
        vd->activity_count = 0;
        vd->start_time = 0;
        vd->voting_active = 1;
        vd->results_visible = 0;
        writer_exit();
        add_activity("Admin reset the election");
        safe_log("Admin command: reset\n");
    }
    else if (strncmp(buf, "set_workers ", 12) == 0) {
        int n = atoi(buf + 12);
        if (n >= 1 && n <= 32) {
            set_worker_pool_size(n);
        } else {
            safe_log("Invalid worker count: %s\n", buf+12);
        }
    }
    else if (strcmp(buf, "enable_pool") == 0) {
        if (!worker_threads) init_worker_pool();
        else use_worker_pool = 1;
        add_activity("Worker pool enabled");
        safe_log("Admin command: enable_pool\n");
    }
    else if (strcmp(buf, "disable_pool") == 0) {
        use_worker_pool = 0;
        add_activity("Worker pool disabled (fallback to direct threads)");
        safe_log("Admin command: disable_pool\n");
    }
    else {
        safe_log("Unknown admin command: %s\n", buf);
    }
}

void *fifo_listener(void *arg) {
    (void)arg;
    char buffer[256];
    while (fifo_running) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(fifo_fd, &fds);
        struct timeval tv = {1, 0};
        int ret = select(fifo_fd+1, &fds, NULL, NULL, &tv);
        if (ret > 0 && FD_ISSET(fifo_fd, &fds)) {
            int n = read(fifo_fd, buffer, sizeof(buffer)-1);
            if (n > 0) {
                buffer[n] = '\0';
                process_admin_command(buffer);
            } else if (n == 0) {
                /* pipe closed by writer, reopen */
                close(fifo_fd);
                fifo_fd = open(FIFO_PATH, O_RDWR);
                if (fifo_fd == -1) {
                    usleep(500000);
                    fifo_fd = open(FIFO_PATH, O_RDWR);
                }
            }
        }
    }
    return NULL;
}

/* ==================== AUTO MODE (modified to use worker pool) ==================== */
void *auto_voter_fn(void *arg) {
    ThreadArg *ta = (ThreadArg*)arg;
    if (use_worker_pool && worker_threads) {
        push_vote_task(ta->vid, ta->cid);
        free(ta);
        /* Note: tasks_completed is incremented by worker threads */
        return NULL;
    } else {
        cast_vote(ta->vid, ta->cid);
        free(ta);
        return NULL;
    }
}

void auto_start(void) {
    create_log_file("Auto");
    safe_log("AUTO MODE: %d voters\n",auto_n);

    vd->total_voters    = auto_n;
    vd->voters_completed= 0;
    vd->start_time      = time(NULL);
    vd->activity_count  = 0;

    clock_gettime(CLOCK_MONOTONIC,&auto_ts_start);
    auto_done_flag  = 0;

    if (use_worker_pool && worker_threads) {
        /* Push all tasks to the queue and let workers process */
        pthread_mutex_lock(&pool_stats_mutex);
        tasks_completed = 0;
        total_tasks_expected = auto_n;
        pthread_mutex_unlock(&pool_stats_mutex);

        for (int i = 0; i < auto_n; i++) {
            push_vote_task(auto_entries[i].vid, auto_entries[i].cid);
        }
        /* No threads to join – we'll poll tasks_completed in auto_update */
    } else if (auto_use_threads) {
        /* Original thread-per-voter */
        for (int i=0;i<auto_n;i++) {
            ThreadArg *ta = malloc(sizeof(ThreadArg));
            ta->vid = auto_entries[i].vid;
            ta->cid = auto_entries[i].cid;
            pthread_create(&auto_tids[i],NULL,auto_voter_fn,ta);
        }
    } else {
        /* Original fork mode */
        for (int i=0;i<auto_n;i++) {
            pid_t pid = fork();
            if (pid==0) {
                if (log_file) { fclose(log_file); log_file=NULL; }
                cast_vote(auto_entries[i].vid, auto_entries[i].cid);
                _exit(0);
            }
        }
    }
}

void auto_update(void) {
    if (auto_done_flag) return;
    int done;

    if (use_worker_pool && worker_threads) {
        pthread_mutex_lock(&pool_stats_mutex);
        done = tasks_completed;
        pthread_mutex_unlock(&pool_stats_mutex);
        /* Also update vd->voters_completed for progress bar */
        sem_wait(g_mutex);
        vd->voters_completed = done;
        sem_post(g_mutex);
    } else {
        sem_wait(g_mutex);
        done = vd->voters_completed;
        sem_post(g_mutex);
    }

    if (use_worker_pool && worker_threads) {
        if (done >= auto_n) {
            struct timespec end; clock_gettime(CLOCK_MONOTONIC,&end);
            auto_elapsed = (end.tv_sec-auto_ts_start.tv_sec)
                         + (end.tv_nsec-auto_ts_start.tv_nsec)/1e9;
            save_perf("Auto-WorkerPool", auto_n, auto_elapsed, 1);
            auto_done_flag = 1;
        }
    } else if (auto_use_threads) {
        if (done >= auto_n) {
            for (int i=0;i<auto_n;i++) pthread_join(auto_tids[i],NULL);
            struct timespec end; clock_gettime(CLOCK_MONOTONIC,&end);
            auto_elapsed = (end.tv_sec-auto_ts_start.tv_sec)
                         + (end.tv_nsec-auto_ts_start.tv_nsec)/1e9;
            save_perf("Auto",auto_n,auto_elapsed,1);
            auto_done_flag = 1;
        }
    } else {
        int status, reaped=0;
        while (waitpid(-1,&status,WNOHANG)>0) reaped++;
        if (done >= auto_n || reaped >= auto_n) {
            while(waitpid(-1,NULL,WNOHANG)>0);
            struct timespec end; clock_gettime(CLOCK_MONOTONIC,&end);
            auto_elapsed = (end.tv_sec-auto_ts_start.tv_sec)
                         + (end.tv_nsec-auto_ts_start.tv_nsec)/1e9;
            save_perf("Auto",auto_n,auto_elapsed,0);
            auto_done_flag = 1;
        }
    }
}

float sx(void) { sfVector2u s=sfRenderWindow_getSize(win); return (float)s.x/BASE_W; }
float sy(void) { sfVector2u s=sfRenderWindow_getSize(win); return (float)s.y/BASE_H; }
float sp(float v) { float f=sx()<sy()?sx():sy(); return v*f; }

/* ==================== SFML DRAW HELPERS ==================== */
/* ==================== SFML DRAW HELPERS ==================== */
void draw_rect(float x,float y,float w,float h,sfColor fill,sfColor out2,float ot) {
    sfRectangleShape *r=sfRectangleShape_create();
    sfRectangleShape_setPosition(r,(sfVector2f){x*sx(),y*sy()});
    sfRectangleShape_setSize(r,(sfVector2f){w*sx(),h*sy()});
    sfRectangleShape_setFillColor(r,fill);
    if(ot>0){sfRectangleShape_setOutlineColor(r,out2);sfRectangleShape_setOutlineThickness(r,ot*sx());}
    sfRenderWindow_drawRectangleShape(win,r,NULL);
    sfRectangleShape_destroy(r);
}
void draw_text(const char *s,float x,float y,unsigned sz,sfColor c,int cen_x,float cxv){
    if(!s||!*s) return;
    sfText *t=sfText_create();
    sfText_setFont(t,fnt); sfText_setString(t,s);
    sfText_setCharacterSize(t,(unsigned)(sz*sp(1.f)));
    sfText_setFillColor(t,c); sfText_setStyle(t,sfTextBold);
    if(cen_x){sfFloatRect b=sfText_getLocalBounds(t);sfText_setPosition(t,(sfVector2f){(cxv-b.width/2.f/sx())*sx(),y*sy()});}
    else sfText_setPosition(t,(sfVector2f){x*sx(),y*sy()});
    sfRenderWindow_drawText(win,t,NULL); sfText_destroy(t);
}
void draw_btn(const char *lbl, float x, float y, float w, float h, sfColor bg, sfColor fg, int sel){
    sfColor actualBg = bg;
    if (sel) {
        // Brighten by adding 40 to RGB
        actualBg.r = (bg.r + 40 > 255) ? 255 : bg.r + 40;
        actualBg.g = (bg.g + 40 > 255) ? 255 : bg.g + 40;
        actualBg.b = (bg.b + 40 > 255) ? 255 : bg.b + 40;
    }
    draw_rect(x, y, w, h, actualBg, C_ACCENT2, sel ? 2.f : 1.f);
    if (sel) draw_rect(x + 2, y + 2, w - 4, 2, COL(255, 255, 255, 60), C_TRANS, 0);
    
    sfText *t = sfText_create();
    sfText_setFont(t, fnt); 
    sfText_setString(t, lbl);
    sfText_setCharacterSize(t, (unsigned)(16 * sp(1.f)));
    sfText_setFillColor(t, fg);
    sfText_setStyle(t, sfTextBold);
    sfFloatRect b = sfText_getLocalBounds(t);
    sfText_setPosition(t, (sfVector2f){(x + w/2.f - b.width/(2.f*sx()))*sx(), 
                                        (y + h/2.f - b.height/(2.f*sy()) - 3)*sy()});
    sfRenderWindow_drawText(win, t, NULL);
    sfText_destroy(t);
}
void draw_input(const char *lbl,const char *val,float x,float y,float w,int active,int masked){
    sfColor bdr=active?C_ACCENT2:C_BORDER;
    draw_text(lbl,x,y,13,C_LGRAY,0,0);
    draw_rect(x,y+20,w,44,active?C_PANEL:C_DGRAY,bdr,active?2.f:1.f);
    if(active) draw_rect(x,y+20,3,44,C_ACCENT,C_TRANS,0);
    char disp[256];
    if(masked&&strlen(val)>0){int L=strlen(val);for(int i=0;i<L&&i<255;i++)disp[i]='*';disp[L<255?L:255]='\0';}
    else{strncpy(disp,val,255);disp[255]='\0';}
    if(active&&(int)(sfTime_asSeconds(sfClock_getElapsedTime(clk))*2)%2==0){int L=strlen(disp);if(L<254){disp[L]='|';disp[L+1]='\0';}}
    draw_text(disp,x+14,y+33,16,active?C_WHITE:C_LGRAY,0,0);
}
void draw_card(float x, float y, float w, float h, sfColor fill, sfColor border) {
    draw_rect(x, y, w, h, fill, border, 2.f);
    // Glass overlay for premium look
    draw_rect(x + 2, y + 2, w - 4, h - 4, C_GLASS, C_TRANS, 0);
}
void draw_divider(float x,float y,float w){draw_rect(x,y,w,1,C_BORDER,C_TRANS,0);}
void draw_badge(const char *txt,float x,float y,sfColor bg){
    sfText *t=sfText_create(); sfText_setFont(t,fnt); sfText_setString(t,txt);
    sfText_setCharacterSize(t,(unsigned)(12*sp(1.f))); sfText_setFillColor(t,C_WHITE);
    sfFloatRect b=sfText_getLocalBounds(t);
    float pw=b.width/sx()+20,ph=22;
    draw_rect(x-pw/2,y,pw,ph,bg,C_TRANS,0);
    sfText_setPosition(t,(sfVector2f){(x-b.width/2.f/sx())*sx(),(y+4)*sy()});
    sfRenderWindow_drawText(win,t,NULL); sfText_destroy(t);
}
void draw_header(const char *title, const char *sub){
    draw_rect(0, 0, BASE_W, 70, C_HEADER, C_TRANS, 0);
    draw_rect(0, 0, BASE_W, 3, C_ACCENT, C_TRANS, 0);
    draw_rect(0, 67, BASE_W, 1, C_BORDER, C_TRANS, 0);
    draw_text(title, BASE_W/2.f, 14, 24, C_WHITE, 1, BASE_W/2.f);
    if(sub && *sub) draw_text(sub, BASE_W/2.f, 44, 13, C_ACCENT2, 1, BASE_W/2.f);
    
    // Add subtle glow line under header
    draw_rect(0, 70, BASE_W, 1, COL(59, 130, 246, 50), C_TRANS, 0);
}
void draw_footer(const char *hint){
    draw_rect(0,BASE_H-36,BASE_W,36,C_HEADER,C_TRANS,0);
    draw_rect(0,BASE_H-36,BASE_W,1,C_BORDER,C_TRANS,0);
    draw_text(hint,BASE_W/2.f,BASE_H-24,12,C_LGRAY,1,BASE_W/2.f);
}
void draw_progress_bar(float x, float y, float w, float h, float pct, sfColor fg){
    draw_rect(x, y, w, h, C_DGRAY, C_BORDER, 1);
    if (pct > 0.001f) {
        draw_rect(x, y, w * pct, h, fg, C_TRANS, 0);
        // Add shine effect on progress bar
        draw_rect(x, y, w * pct, h/3, COL(255, 255, 255, 60), C_TRANS, 0);
    }
}
void text_input(sfEvent *e,char *buf,int *len,int maxlen,int dig_only){
    if(e->type!=sfEvtTextEntered) return;
    sfUint32 c=e->text.unicode;
    if(c==13||c==27) return;
    if(c==8){if(*len>0){(*len)--;buf[*len]='\0';}return;}
    if(c>=32&&c<127&&*len<maxlen){if(dig_only&&!(c>='0'&&c<='9'))return;buf[*len]=(char)c;(*len)++;buf[*len]='\0';}
}
void draw_splash(void){
    sfRenderWindow_clear(win, C_BG);
    
    // Animated gradient top bar
    for (int i = 0; i < BASE_W; i += 10) {
        sfColor grad = COL(59, 130, 246, 80 + (i * 50 / BASE_W));
        draw_rect(i, 0, 10, 4, grad, C_TRANS, 0);
    }
    
    float cw = 680, ch = 380, cx = (BASE_W-cw)/2.f, cy = (BASE_H-ch)/2.f - 20;
    draw_card(cx, cy, cw, ch, C_PANEL, C_BORDER);
    draw_rect(cx, cy, cw, 4, C_ACCENT, C_TRANS, 0);
    
    // Glowing title effect (draw twice for shadow effect)
    draw_text("VOTING SYSTEM", BASE_W/2.f, cy + 28, 36, COL(59, 130, 246, 80), 1, BASE_W/2.f); // shadow
    draw_text("VOTING SYSTEM", BASE_W/2.f - 1, cy + 27, 36, C_WHITE, 1, BASE_W/2.f); // main text
    
    draw_text("SYNCHRONIZED & SECURE", BASE_W/2.f, cy + 72, 16, C_ACCENT2, 1, BASE_W/2.f);
    draw_divider(cx + 40, cy + 102, cw - 80);
    draw_text("OS Project  |  Shared Memory, Semaphores, Threads, Processes", BASE_W/2.f, cy + 118, 13, C_LGRAY, 1, BASE_W/2.f);
    
    float cw3 = cw/3.f, fy = cy + 158;
    draw_text("Reader-Writer Lock", cx + cw3*0 + cw3/2, fy, 12, C_ACCENT2, 1, cx + cw3*0 + cw3/2);
    draw_text("POSIX Semaphores",   cx + cw3*1 + cw3/2, fy, 12, C_ACCENT2, 1, cx + cw3*1 + cw3/2);
    draw_text("Fork + Threads",     cx + cw3*2 + cw3/2, fy, 12, C_ACCENT2, 1, cx + cw3*2 + cw3/2);
    
    if (splash_timer >= 1.5f){
        // Pulsing "Press any key" button
        sfColor pulseBg = getPulseColor();
        draw_rect(cx + 40, cy + 210, cw - 80, 48, pulseBg, C_ACCENT2, 2.f);
        draw_text("PRESS ANY KEY TO CONTINUE", BASE_W/2.f, cy + 224, 15, C_BLACK, 1, BASE_W/2.f);
        splash_waiting_for_key = 1;
    } else {
        draw_progress_bar(cx + 40, cy + 218, cw - 80, 10, splash_timer / 1.5f, C_ACCENT);
        draw_text("Loading secure environment...", BASE_W/2.f, cy + 248, 12, C_LGRAY, 1, BASE_W/2.f);
    }
    
    draw_text("Secure Connection", BASE_W/2.f, BASE_H - 28, 11, C_DGRAY, 1, BASE_W/2.f);
    draw_rect(0, BASE_H - 3, BASE_W, 3, C_ACCENT, C_TRANS, 0);
}
void go(Screen s) {
    cur_scr = s;
    if (s==SCR_ADMIN_LOGIN) {
        memset(pw_buf,0,sizeof(pw_buf)); pw_len=0; admin_tries=3;
        memset(pw_err,0,sizeof(pw_err));
    }
    if (s==SCR_ADMIN_PANEL) adm_sel=0;
    if (s==SCR_ADMIN_CANDIDATES) { cand_sel=0; cand_menu_sel=0; del_idx=-1; }
    if (s==SCR_ADMIN_ADD_CAND) {
        memset(new_cand,0,sizeof(new_cand));
        new_cand_len=0;
        add_cand_sel = 0;
    }
    if (s==SCR_MANUAL_VOTER_ID) {
        memset(vid_buf,0,sizeof(vid_buf)); vid_len=0; vid_val=0;
        memset(vid_err,0,sizeof(vid_err));
        vote_sel=0;
        confirm_vote_show = 0;
        memset(manual_city,0,sizeof(manual_city));
        manual_city_len = 0;
        manual_field_sel = 0;
    }
    if (s==SCR_MANUAL_VOTE) {
        vote_sel=0; memset(vote_err,0,sizeof(vote_err));
        confirm_vote_show = 0;
    }
    if (s==SCR_MANUAL_SUCCESS) {
        manual_success_sel = 0;
    }
    if (s==SCR_AUTO_SETUP_CITIES) {
        memset(city1,0,sizeof(city1)); city1_len=0;
        memset(city2,0,sizeof(city2)); city2_len=0;
        auto_field=0; auto_n=0;
        memset(auto_setup_err,0,sizeof(auto_setup_err));
    }
    if (s==SCR_AUTO_VOTER_COUNT) {
        memset(nv_buf,0,sizeof(nv_buf)); nv_len=0;
    }
    if (s==SCR_AUTO_VOTER_DETAIL) {
        auto_cur=0;
        memset(avid_buf,0,sizeof(avid_buf)); avid_len=0;
        acand_sel=0;
        memset(avid_err,0,sizeof(avid_err));
        memset(auto_entries,0,sizeof(auto_entries));
    }
    if (s==SCR_AUTO_RUNNING) {
        auto_done_flag=0; auto_elapsed=0.0;
        auto_complete_sel = 0;
        auto_start();
    }
    if (s==SCR_RESULTS) {
        results_back_sel = 0;
    }
}

void go_msg(const char *txt,Screen ret,float autod) {
    strncpy(msg_text,txt,511); msg_text[511]='\0';
    msg_ret   = ret;
    msg_timer = autod;
    cur_scr   = SCR_MSG;
}

void go_confirm(const char *title,const char *body,Screen yes,Screen no) {
    strncpy(confirm_title,title,127); confirm_title[127]='\0';
    strncpy(confirm_body, body, 255); confirm_body[255]='\0';
    confirm_yes = yes;
    confirm_no  = no;
    cur_scr     = SCR_ADMIN_RESET_CONFIRM;
}

void go_password_confirm(int action) {
    pending_action = action;
    memset(pw_buf,0,sizeof(pw_buf));
    pw_len = 0;
    memset(pw_err,0,sizeof(pw_err));
    admin_tries = 3;
    cur_scr = SCR_ADMIN_PASSWORD_CONFIRM;
}

/* ==================== RENDERERS ==================== */
/* ==================== REDESIGNED RENDERERS ==================== */
void render_main(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("SYNCHRONIZED VOTING SYSTEM","Secure, Reliable &amp; Transparent");
    
    char sts[128];
    snprintf(sts, sizeof(sts), " Votes: %d   |   ", vd->total_votes);
    
    if (vd->voting_active) {
        sfColor pulseC = getPulseColor();
        draw_rect(0, 72, BASE_W, 32, pulseC, C_TRANS, 0);
        strcat(sts, " ACTIVE");
    } else {
        draw_rect(0, 72, BASE_W, 32, C_DGRAY, C_TRANS, 0);
        strcat(sts, " ENDED");
    }
    
    char candBuf[32];
    snprintf(candBuf, sizeof(candBuf), "   |   Candidates: %d", vd->candidate_count);
    strcat(sts, candBuf);
    draw_text(sts, BASE_W/2.f, 79, 14, C_BLACK, 1, BASE_W/2.f);
    
    float bw=340,bh=52,gap=14,total_h=5*(bh+gap)-gap,start_y=(BASE_H-total_h)/2.f+18,bx=(BASE_W-bw)/2.f;
    const char *labels[5]={"  Manual Voting","  Auto Mode","  View Results","  Performance Data","  Admin Panel"};
    for(int i=0;i<5;i++){
        float fy=start_y+i*(bh+gap); int sel=(adm_sel==i);
        draw_rect(bx,fy,bw,bh,sel?C_ACCENT:C_CARD,sel?C_ACCENT2:C_BORDER,sel?2.f:1.f);
        if(sel) draw_rect(bx,fy,4,bh,C_ACCENT2,C_TRANS,0);
        draw_text(labels[i],bx+20,fy+bh/2.f-9,16,sel?C_BLACK:C_WHITE,0,0);
    }
    draw_footer("Arrow Keys / Mouse  |  Enter to select  |  ESC to exit");
}
void render_admin_login(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("ADMIN LOGIN","Enter administrator credentials");
    float cw=460,ch=260,cx=(BASE_W-cw)/2.f,cy=140;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_ACCENT,C_TRANS,0);
    draw_input("Password",pw_buf,cx+30,cy+30,cw-60,1,1);
    char att[64]; snprintf(att,sizeof(att),"Attempts remaining: %d",admin_tries);
    draw_text(att,BASE_W/2.f,cy+108,13,admin_tries<=1?C_WARNING:C_LGRAY,1,BASE_W/2.f);
    if(*pw_err) draw_text(pw_err,BASE_W/2.f,cy+130,13,C_WARNING,1,BASE_W/2.f);
    draw_divider(cx+30,cy+155,cw-60);
    draw_btn("LOGIN",cx+cw/2.f-80,cy+167,160,44,C_ACCENT,C_BLACK,1);
    draw_footer("Type password and Enter  |  ESC to go back");
}
void render_admin_panel(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("ADMIN PANEL","Election Management");
    const char *items[7]={"  Live Voter Monitoring","  Manage Candidates","  Start New Election (Reset)","  Export Reports","  View Statistics","  DECLARE RESULTS (End Voting)","  Back to Main Menu"};
    sfColor ic[7]={{155,180,192,255},{155,180,192,255},{210,193,182,255},{155,180,192,255},{155,180,192,255},{210,193,182,255},{130,145,185,255}};
    float bw=480,bh=48,gap=10,total=7*(bh+gap)-gap,sy2=(BASE_H-total)/2.f+5,bx=(BASE_W-bw)/2.f;
    for(int i=0;i<7;i++){
        float fy=sy2+i*(bh+gap); int sel=(adm_sel==i);
        draw_rect(bx,fy,bw,bh,sel?ic[i]:C_CARD,sel?ic[i]:C_BORDER,sel?2.f:1.f);
        draw_rect(bx,fy,4,bh,ic[i],C_TRANS,0);
        draw_text(items[i],bx+16,fy+bh/2.f-9,15,sel?C_BLACK:C_WHITE,0,0);
    }
    draw_footer("Arrow Keys / Mouse  |  Enter to select  |  ESC to go back");
}
void render_admin_candidates(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("MANAGE CANDIDATES","Add or remove election candidates");
    reader_enter();
    float x0=100,y0=88,row=44;
    draw_rect(x0,y0,BASE_W-200,30,C_ACCENT,C_TRANS,0);
    draw_text("#",x0+15,y0+7,13,C_BLACK,0,0); draw_text("Candidate Name",x0+60,y0+7,13,C_BLACK,0,0); draw_text("Votes",x0+440,y0+7,13,C_BLACK,0,0);
    for(int i=0;i<vd->candidate_count&&i<10;i++){
        float ry=y0+32+i*row; int sel=(cand_sel==i);
        draw_rect(x0,ry,BASE_W-200,row-2,sel?C_BTN_HOV:(i%2==0?C_CARD:C_PANEL),sel?C_ACCENT:C_BORDER,sel?2.f:1.f);
        if(sel) draw_rect(x0,ry,3,row-2,C_ACCENT,C_TRANS,0);
        sfColor tc=sel?C_ACCENT2:C_WHITE;
        char num[8]; snprintf(num,sizeof(num),"%d",i+1);
        draw_text(num,x0+15,ry+12,14,tc,0,0); draw_text(vd->candidate_names[i],x0+60,ry+12,14,tc,0,0);
        char vc[16]; snprintf(vc,sizeof(vc),"%d",vd->votes[i]); draw_text(vc,x0+440,ry+12,14,tc,0,0);
    }
    if(vd->candidate_count==0) draw_text("No candidates yet. Click Add Candidate to create one.",x0,y0+50,14,C_LGRAY,0,0);
    reader_exit();
    float by=BASE_H-78,bw=140,bh=42,gap=18,total_w=3*bw+2*gap,start_x=(BASE_W-total_w)/2.f;
    draw_btn("Add Candidate",  start_x,            by,bw,bh,C_SUCCESS,C_WHITE,(cand_menu_sel==0));
    draw_btn("Delete Selected",start_x+bw+gap,     by,bw,bh,COL(239,68,68,255),C_WHITE,(cand_menu_sel==1));
    draw_btn("Back",           start_x+2*(bw+gap), by,bw,bh,C_DGRAY,  C_WHITE,(cand_menu_sel==2));
    draw_footer("Left/Right: menu  |  Up/Down: candidate  |  Enter to select  |  ESC: back");
}
void render_admin_add_cand(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("ADD CANDIDATE","Register a new candidate");
    float cw=500,ch=220,cx=(BASE_W-cw)/2.f,cy=150;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_SUCCESS,C_TRANS,0);
    draw_input("Candidate Name",new_cand,cx+30,cy+28,cw-60,1,0);
    draw_divider(cx+30,cy+155,cw-60);
    draw_btn("Add",   cx+cw/2.f-145,cy+165,130,42,C_SUCCESS,C_WHITE,(add_cand_sel==0));
    draw_btn("Cancel",cx+cw/2.f+15, cy+165,130,42,C_DGRAY,  C_WHITE,(add_cand_sel==1));
    draw_footer("Type name and Enter  |  Left/Right to switch  |  ESC to cancel");
}
void render_admin_live(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("LIVE VOTER MONITORING","Real-time feed");
    reader_enter();
    float lx=30,ly=78,lw=BASE_W/2.f-40,lh=BASE_H-162;
    draw_card(lx,ly,lw,lh,C_CARD,C_BORDER); draw_rect(lx,ly,lw,28,C_ACCENT,C_TRANS,0);
    draw_text("VOTERS WHO VOTED",lx+10,ly+7,12,C_BLACK,0,0);
    char cnt2[32]; snprintf(cnt2,sizeof(cnt2),"(%d)",vd->voted_count); draw_text(cnt2,lx+lw-50,ly+7,12,C_BLACK,0,0);
    int per_row=5,show=vd->voted_count>30?30:vd->voted_count;
    for(int i=0;i<show;i++){
        char id[16]; snprintf(id,sizeof(id),"%d",vd->voted_ids[i]);
        float vx=lx+10+(i%per_row)*((lw-20)/per_row),vy=ly+36+(i/per_row)*28.f;
        draw_rect(vx,vy,(lw-20)/per_row-6,22,C_BTN_HOV,C_ACCENT,1); draw_text(id,vx+8,vy+4,12,C_ACCENT2,0,0);
    }
    if(vd->voted_count==0) draw_text("No votes recorded yet.",lx+10,ly+45,14,C_LGRAY,0,0);
    float rx=BASE_W/2.f+10,ry=78,rw=BASE_W/2.f-40,rh=BASE_H-162;
    draw_card(rx,ry,rw,rh,C_CARD,C_BORDER); draw_rect(rx,ry,rw,28,C_ACCENT,C_TRANS,0);
    draw_text("RECENT ACTIVITY",rx+10,ry+7,12,C_BLACK,0,0);
    int cnt3=vd->activity_count,start=cnt3>12?cnt3-12:0;
    for(int i=start;i<cnt3;i++){
        int idx=i%MAX_ACTIVITY_LOG; float ay=ry+36+(i-start)*22.f;
        if(ay>ry+rh-20) break;
        draw_text(vd->last_activity[idx],rx+10,ay,12,i==cnt3-1?C_ACCENT2:C_WHITE,0,0);
    }
    if(cnt3==0) draw_text("No activity yet.",rx+10,ry+45,14,C_LGRAY,0,0);
    reader_exit();
    char summ[128]; snprintf(summ,sizeof(summ),"Total Turnout: %d   |   Status: %s",vd->voted_count,vd->voting_active?"ACTIVE":"ENDED");
    draw_rect(0,BASE_H-78,BASE_W,34,C_ACCENT,C_TRANS,0); draw_text(summ,BASE_W/2.f,BASE_H-65,14,C_BLACK,1,BASE_W/2.f);
    draw_btn("Back",BASE_W/2.f-55,BASE_H-42,110,34,C_DGRAY,C_WHITE,1);
    draw_footer("ESC or Back to return");
}
static void draw_stat_row(float cx, float *y, const char *k, const char *v, sfColor vc){
    float rowW = 280;
    float rowH = 38;
    float startX = cx - rowW/2;
    
    draw_rect(startX, *y, rowW, rowH, C_CARD, C_BORDER, 1);
    draw_rect(startX, *y, 3, rowH, C_ACCENT, C_TRANS, 0);
    
    // Key (left aligned)
    draw_text(k, startX + 12, *y + 12, 13, C_LGRAY, 0, 0);
    
    // Value (right aligned within the box)
    float valWidth = 120;
    draw_text(v, startX + rowW - valWidth, *y + 12, 13, vc, 0, 0);
    
    *y += 46;
}
void render_admin_stats(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("SYSTEM STATISTICS","Election overview");
    reader_enter();
    
    float leftColX = BASE_W/2.f - 160;  // Left column center
    float rightColX = BASE_W/2.f + 160; // Right column center
    float y = 90;
    
    // LEFT COLUMN
    char buf[128];
    snprintf(buf,sizeof(buf),"%d",vd->candidate_count);
    draw_stat_row(leftColX, &y, "Total Candidates:", buf, C_ACCENT2);
    
    snprintf(buf,sizeof(buf),"%d",vd->total_votes);
    draw_stat_row(leftColX, &y, "Total Votes Cast:", buf, C_SUCCESS);
    
    snprintf(buf,sizeof(buf),"%d",vd->voted_count);
    draw_stat_row(leftColX, &y, "Unique Voters:", buf, C_WHITE);
    
    // Reset Y for right column
    y = 90;
    
    // RIGHT COLUMN
    draw_stat_row(rightColX, &y, "Voting Status:", vd->voting_active ? "ACTIVE" : "ENDED", 
                  vd->voting_active ? C_SUCCESS : C_WARNING);
    
    draw_stat_row(rightColX, &y, "Results Visible:", vd->results_visible ? "YES" : "NO", 
                  vd->results_visible ? C_SUCCESS : C_WARNING);
    
    if(vd->start_time > 0){
        int el = (int)(time(NULL) - vd->start_time);
        snprintf(buf,sizeof(buf),"%d sec",el);
        draw_stat_row(rightColX, &y, "Elapsed Time:", buf, C_WHITE);
    }
    
    // CANDIDATE BREAKDOWN SECTION
    float candY = 240;
    draw_text("CANDIDATE BREAKDOWN", BASE_W/2.f, candY, 16, C_ACCENT, 1, BASE_W/2.f);
    draw_divider(BASE_W/2.f - 300, candY + 22, 600);
    candY += 40;
    
    float tableStartX = BASE_W/2.f - 280;
    
    // Table header
    draw_rect(tableStartX, candY, 560, 30, C_ACCENT, C_TRANS, 0);
    draw_text("#", tableStartX + 20, candY + 8, 12, C_BLACK, 0, 0);
    draw_text("Candidate", tableStartX + 60, candY + 8, 12, C_BLACK, 0, 0);
    draw_text("Votes", tableStartX + 420, candY + 8, 12, C_BLACK, 0, 0);
    draw_text("%", tableStartX + 510, candY + 8, 12, C_BLACK, 0, 0);
    candY += 36;
    
    for(int i = 0; i < vd->candidate_count && candY < BASE_H - 100; i++){
        float pct = vd->total_votes > 0 ? (float)vd->votes[i] / vd->total_votes * 100.f : 0.f;
        
        draw_rect(tableStartX, candY, 560, 34, (i % 2 == 0) ? C_CARD : C_PANEL, C_BORDER, 1);
        
        char num[8]; 
        snprintf(num, sizeof(num), "%d", i+1);
        draw_text(num, tableStartX + 20, candY + 10, 13, C_WHITE, 0, 0);
        
        draw_text(vd->candidate_names[i], tableStartX + 60, candY + 10, 13, C_WHITE, 0, 0);
        
        char vc[16]; 
        snprintf(vc, sizeof(vc), "%d", vd->votes[i]);
        draw_text(vc, tableStartX + 420, candY + 10, 13, C_ACCENT2, 0, 0);
        
        char pc[16]; 
        snprintf(pc, sizeof(pc), "%.1f%%", pct);
        draw_text(pc, tableStartX + 510, candY + 10, 13, C_SUCCESS, 0, 0);
        
        candY += 40;
    }
    
    // Back button
    draw_btn("Back", BASE_W/2.f - 55, BASE_H - 78, 110, 36, C_DGRAY, C_WHITE, 1);
    
    reader_exit();
    draw_footer("ESC or Back to return");
}
void render_confirm(void){
    draw_rect(0,0,BASE_W,BASE_H,C_SHADOW,C_TRANS,0);
    float dw=520,dh=235,dx=(BASE_W-dw)/2.f,dy=(BASE_H-dh)/2.f;
    draw_card(dx,dy,dw,dh,C_PANEL,C_WARNING); draw_rect(dx,dy,dw,4,C_WARNING,C_TRANS,0);
    draw_text(confirm_title,BASE_W/2.f,dy+18,19,C_WARNING,1,BASE_W/2.f);
    draw_divider(dx+30,dy+50,dw-60); draw_text(confirm_body,BASE_W/2.f,dy+68,14,C_WHITE,1,BASE_W/2.f);
    float bw=180,bh=44,gap=20,bx=BASE_W/2.f-bw-gap/2,by=dy+dh-62;
    draw_btn("YES - Confirm",bx,      by,bw,bh,confirm_selection==0?C_SUCCESS:C_DGRAY,confirm_selection==0?C_WHITE:C_LGRAY,(confirm_selection==0));
    draw_btn("NO - Cancel",  bx+bw+gap,by,bw,bh,confirm_selection==1?C_WARNING:C_DGRAY,confirm_selection==1?C_WHITE:C_LGRAY,(confirm_selection==1));
    draw_footer("Left/Right: switch  |  Enter: confirm  |  Y=YES  N=NO  |  ESC=Cancel");
}
void render_password_confirm(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("ADMIN VERIFICATION","Password required to proceed");
    float cw=480,ch=250,cx=(BASE_W-cw)/2.f,cy=150;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_WARNING,C_TRANS,0);
    draw_text("This action requires admin password",BASE_W/2.f,cy+20,13,C_WARNING,1,BASE_W/2.f);
    draw_input("Password",pw_buf,cx+30,cy+48,cw-60,1,1);
    char att[64]; snprintf(att,sizeof(att),"Attempts remaining: %d",admin_tries);
    draw_text(att,BASE_W/2.f,cy+138,13,admin_tries<=1?C_WARNING:C_LGRAY,1,BASE_W/2.f);
    if(*pw_err) draw_text(pw_err,BASE_W/2.f,cy+160,13,C_WARNING,1,BASE_W/2.f);
    draw_divider(cx+30,cy+188,cw-60);
    draw_btn("Confirm Action",cx+cw/2.f-90,cy+198,180,42,C_ACCENT,C_BLACK,1);
    draw_footer("Type password and Enter  |  ESC to cancel");
}
void render_report_password_confirm(void){render_password_confirm();}
void render_manual_vid(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("MANUAL VOTING","Step 1 of 2 - Enter your details");
    float cw=540,ch=290,cx=(BASE_W-cw)/2.f,cy=108;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_ACCENT,C_TRANS,0);
    draw_input("Voter ID (1000-9999)",vid_buf,cx+30,cy+22,cw-60,(manual_field_sel==0),0);
    draw_input("City / Location",manual_city,cx+30,cy+112,cw-60,(manual_field_sel==1),0);
    if(*vid_err) draw_text(vid_err,BASE_W/2.f,cy+222,13,C_WARNING,1,BASE_W/2.f);
    draw_divider(cx+30,cy+250,cw-60);
    draw_btn("NEXT",cx+cw/2.f-75,cy+260,150,42,C_ACCENT,C_BLACK,1);
    draw_footer("Up/Down: switch fields  |  Enter or Next  |  ESC to go back");
}
void render_manual_vote(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("MANUAL VOTING","Step 2 of 2 - Select your candidate");
    char sub[128]; snprintf(sub,sizeof(sub),"Voter ID: %d   |   City: %s",vid_val,manual_city);
    draw_rect(0,72,BASE_W,28,C_PANEL,C_TRANS,0); draw_text(sub,BASE_W/2.f,78,13,C_ACCENT2,1,BASE_W/2.f);
    reader_enter();
    if(!confirm_vote_show){
        float bw=500,bh=50,gap=10,total=vd->candidate_count*(bh+gap)-gap,start_y=(BASE_H-total)/2.f+28,bx=(BASE_W-bw)/2.f;
        for(int i=0;i<vd->candidate_count;i++){
            float fy=start_y+i*(bh+gap); int sel=(vote_sel==i);
            draw_rect(bx,fy,bw,bh,sel?C_ACCENT:C_CARD,sel?C_ACCENT2:C_BORDER,sel?2.f:1.f);
            if(sel) draw_rect(bx,fy,4,bh,C_ACCENT2,C_TRANS,0);
            char lbl[80]; snprintf(lbl,sizeof(lbl),"%d.  %s",i+1,vd->candidate_names[i]);
            draw_text(lbl,bx+20,fy+bh/2.f-9,16,sel?C_BLACK:C_WHITE,0,0);
            if(sel) draw_text("SELECTED",bx+bw-100,fy+bh/2.f-9,12,C_ACCENT2,0,0);
        }
        if(*vote_err) draw_text(vote_err,BASE_W/2.f,BASE_H-100,13,C_WARNING,1,BASE_W/2.f);
        draw_btn("Back",BASE_W/2.f-55,BASE_H-80,110,36,C_DGRAY,C_WHITE,0);
        draw_footer("Arrow Keys: select  |  Enter: confirm  |  ESC: back");
    } else {
        float dw=520,dh=230,dx=(BASE_W-dw)/2.f,dy=(BASE_H-dh)/2.f-10;
        draw_card(dx,dy,dw,dh,C_PANEL,C_ACCENT); draw_rect(dx,dy,dw,4,C_ACCENT,C_TRANS,0);
        draw_text("CONFIRM YOUR VOTE",BASE_W/2.f,dy+16,18,C_ACCENT2,1,BASE_W/2.f);
        draw_divider(dx+30,dy+48,dw-60);
        char l1[80],l2[80],l3[80];
        snprintf(l1,sizeof(l1),"Voter ID:   %d",vid_val);
        snprintf(l2,sizeof(l2),"City:       %s",manual_city);
        snprintf(l3,sizeof(l3),"Candidate:  %s",vd->candidate_names[vote_sel]);
        draw_text(l1,dx+50,dy+64, 14,C_LGRAY,  0,0);
        draw_text(l2,dx+50,dy+88, 14,C_LGRAY,  0,0);
        draw_text(l3,dx+50,dy+112,15,C_ACCENT2,0,0);
        draw_divider(dx+30,dy+145,dw-60);
        draw_btn("CONFIRM VOTE",dx+30,    dy+dh-58,210,46,C_SUCCESS,C_WHITE,1);
        draw_btn("CANCEL",      dx+dw-240,dy+dh-58,210,46,C_WARNING,C_WHITE,0);
        draw_footer("Enter: confirm  |  ESC or Cancel to go back");
    }
    reader_exit();
}
void render_manual_success(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("VOTE RECORDED","Your vote has been successfully cast");
    float cw=500,ch=230,cx=(BASE_W-cw)/2.f,cy=128;
    draw_card(cx,cy,cw,ch,C_CARD,C_SUCCESS); draw_rect(cx,cy,cw,4,C_SUCCESS,C_TRANS,0);
    draw_text("VOTE CAST SUCCESSFULLY",BASE_W/2.f,cy+20,19,C_SUCCESS,1,BASE_W/2.f);
    draw_divider(cx+30,cy+52,cw-60);
    char l1[128],l2[128],l3[128];
    snprintf(l1,sizeof(l1),"Voter ID:    %d",vid_val);
    snprintf(l2,sizeof(l2),"City:        %s",manual_city);
    snprintf(l3,sizeof(l3),"Voted For:   %s",vd->candidate_names[voted_candidate]);
    draw_text(l1,cx+60,cy+68, 14,C_LGRAY,  0,0);
    draw_text(l2,cx+60,cy+90, 14,C_LGRAY,  0,0);
    draw_text(l3,cx+60,cy+114,15,C_ACCENT2,0,0);
    float bw=190,gap=25,start_x=(BASE_W-(2*bw+gap))/2.f;
    draw_divider(cx+30,cy+155,cw-60);
    draw_btn("Vote Again",start_x,       cy+163,bw,42,C_ACCENT,C_BLACK,(manual_success_sel==0));
    draw_btn("Main Menu", start_x+bw+gap,cy+163,bw,42,C_DGRAY, C_WHITE,(manual_success_sel==1));
    draw_footer("Left/Right: select  |  Enter to confirm");
}
void render_auto_setup_cities(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("AUTO MODE SETUP","Step 1 of 3 - Enter city codes");
    float cw=540,ch=330,cx=(BASE_W-cw)/2.f,cy=90;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_ACCENT,C_TRANS,0);
    draw_input("City / Postal Code 1",city1,cx+30,cy+22, cw-60,auto_field==0,0);
    draw_input("City / Postal Code 2",city2,cx+30,cy+112,cw-60,auto_field==1,0);
    if(*city1&&*city2){
        char c1[50],c2[50]; strncpy(c1,city1,49);c1[49]=0; strncpy(c2,city2,49);c2[49]=0;
        for(int i=0;c1[i];i++)c1[i]=tolower((unsigned char)c1[i]);
        for(int i=0;c2[i];i++)c2[i]=tolower((unsigned char)c2[i]);
        auto_use_threads=(strcmp(c1,c2)==0);
        const char *mode=auto_use_threads?"Same city -> Thread Mode":"Different cities -> Process Mode";
        sfColor mc=auto_use_threads?C_SUCCESS:C_ACCENT2;
        draw_rect(cx+30,cy+215,cw-60,30,auto_use_threads?COL(20,60,30,255):COL(20,40,70,255),mc,1);
        draw_text(mode,BASE_W/2.f,cy+223,12,mc,1,BASE_W/2.f);
    }
    if(*auto_setup_err) draw_text(auto_setup_err,BASE_W/2.f,cy+258,13,C_WARNING,1,BASE_W/2.f);
    draw_divider(cx+30,cy+275,cw-60);
    draw_btn("NEXT",cx+cw/2.f-75,cy+283,150,44,C_ACCENT,C_BLACK,1);
    draw_footer("Up/Down: switch fields  |  Enter to proceed  |  ESC to go back");
}
void render_auto_voter_count(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("AUTO MODE SETUP","Step 2 of 3 - Number of voters");
    float cw=480,ch=260,cx=(BASE_W-cw)/2.f,cy=130;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_ACCENT,C_TRANS,0);
    char mt[64]; snprintf(mt,sizeof(mt),"Mode: %s",auto_use_threads?"Thread Mode":"Process Mode");
    draw_badge(mt,BASE_W/2.f,cy+16,auto_use_threads?COL(30,120,60,255):COL(30,80,150,255));
    draw_input("Number of Voters (1-100)",nv_buf,cx+30,cy+52,cw-60,1,0);
    if(*auto_setup_err) draw_text(auto_setup_err,BASE_W/2.f,cy+152,13,C_WARNING,1,BASE_W/2.f);
    draw_divider(cx+30,cy+196,cw-60);
    draw_btn("NEXT",cx+cw/2.f-75,cy+206,150,44,C_ACCENT,C_BLACK,1);
    draw_footer("Type number and Enter  |  ESC to go back");
}
void render_auto_voter_detail(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    char hdr[64]; snprintf(hdr,sizeof(hdr),"Voter %d of %d",auto_cur+1,auto_n);
    draw_header("AUTO MODE - VOTER DETAILS",hdr);
    draw_progress_bar(80,76,BASE_W-160,8,(float)auto_cur/auto_n,C_SUCCESS);
    float cw=540,ch=340,cx=(BASE_W-cw)/2.f,cy=98;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,C_ACCENT,C_TRANS,0);
    draw_input("Voter ID (1000-9999)",avid_buf,cx+30,cy+16,cw-60,1,0);
    draw_text("Select Candidate:",cx+30,cy+106,13,C_ACCENT,0,0);
    draw_divider(cx+30,cy+124,cw-60);
    reader_enter();
    for(int i=0;i<vd->candidate_count&&i<6;i++){
        float fy=cy+132+i*32; int sel=(acand_sel==i);
        draw_rect(cx+30,fy,cw-60,26,sel?C_ACCENT:C_DGRAY,sel?C_ACCENT2:C_BORDER,sel?2.f:1.f);
        if(sel) draw_rect(cx+30,fy,3,26,C_ACCENT2,C_TRANS,0);
        char lbl[64]; snprintf(lbl,sizeof(lbl),"%d.  %s",i+1,vd->candidate_names[i]);
        draw_text(lbl,cx+42,fy+5,13,sel?C_BLACK:C_WHITE,0,0);
    }
    reader_exit();
    if(*avid_err) draw_text(avid_err,BASE_W/2.f,BASE_H-140,13,C_WARNING,1,BASE_W/2.f);
    const char *bl=auto_cur<auto_n-1?"Next Voter":"Start Voting";
    draw_btn(bl,BASE_W/2.f-90,BASE_H-88,180,44,auto_cur<auto_n-1?C_ACCENT:C_SUCCESS,C_BLACK,1);
    draw_footer("Arrow Keys: select candidate  |  Enter to confirm voter");
}
void render_auto_running(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("AUTO MODE - RUNNING",auto_use_threads?"Thread Mode":"Process Mode");
    int done; sem_wait(g_mutex); done=vd->voters_completed; sem_post(g_mutex);
    float pct=auto_n>0?(float)done/auto_n:0.f;
    float cw=560,ch=270,cx=(BASE_W-cw)/2.f,cy=128;
    draw_card(cx,cy,cw,ch,C_CARD,C_BORDER); draw_rect(cx,cy,cw,3,auto_use_threads?C_SUCCESS:C_ACCENT,C_TRANS,0);
    char prog[64]; snprintf(prog,sizeof(prog),"%d / %d voters processed",done,auto_n);
    draw_text(prog,BASE_W/2.f,cy+26,20,C_WHITE,1,BASE_W/2.f);
    draw_progress_bar(cx+40,cy+62,cw-80,20,pct,C_SUCCESS);
    char pcts[16]; snprintf(pcts,sizeof(pcts),"%.0f%%",pct*100.f); draw_text(pcts,BASE_W/2.f,cy+90,16,C_ACCENT2,1,BASE_W/2.f);
    if(auto_done_flag){
        draw_rect(cx+40,cy+120,cw-80,38,COL(20,70,30,255),C_SUCCESS,2);
        draw_text("VOTING COMPLETED!",BASE_W/2.f,cy+132,18,C_SUCCESS,1,BASE_W/2.f);
        char es[64]; snprintf(es,sizeof(es),"Time: %.4f seconds",auto_elapsed);
        draw_text(es,BASE_W/2.f,cy+168,13,C_LGRAY,1,BASE_W/2.f);
        float bw=190,gap=25,start_x=(BASE_W-(2*bw+gap))/2.f;
        draw_btn("Vote Again",start_x,       cy+190,bw,42,C_ACCENT,C_BLACK,(auto_complete_sel==0));
        draw_btn("Main Menu", start_x+bw+gap,cy+190,bw,42,C_DGRAY, C_WHITE,(auto_complete_sel==1));
        draw_footer("Left/Right: select  |  Enter to confirm");
    } else { draw_text("Processing votes in parallel...",BASE_W/2.f,cy+128,15,C_LGRAY,1,BASE_W/2.f); }
}
void render_results(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("ELECTION RESULTS","Final vote tally");
    reader_enter();
    
    if(!vd->results_visible){
        float cw=460,ch=160,cx=(BASE_W-cw)/2.f,cy=(BASE_H-ch)/2.f;  // Centered vertically
        draw_card(cx,cy,cw,ch,C_CARD,C_WARNING);
        draw_rect(cx,cy,cw,4,C_WARNING,C_TRANS,0);
        draw_text("RESULTS LOCKED",BASE_W/2.f,cy+35,22,C_WARNING,1,BASE_W/2.f);
        draw_text("Admin must declare results to unlock.",BASE_W/2.f,cy+72,13,C_LGRAY,1,BASE_W/2.f);
        draw_btn("Back",BASE_W/2.f-55,cy+108,110,36,C_DGRAY,C_WHITE,1);
        reader_exit();
        draw_footer("ESC or Back to return");
        return;
    }
    
    int wi=-1,wv=-1,tie=0;
    for(int i=0;i<vd->candidate_count;i++){
        if(vd->votes[i]>wv){
            wv=vd->votes[i];
            wi=i;
            tie=0;
        } else if(vd->votes[i]==wv && wv>0){
            tie=1;
        }
    }
    
    // Calculate total height of all content
    float contentHeight = 0;
    float startY = 100;  // Initial Y position (will adjust)
    
    // Winner card height (if winner exists)
    if(vd->total_votes > 0 && !tie && wi >= 0){
        contentHeight += 80;  // Winner card takes ~80px
    } else if(tie){
        contentHeight += 70;  // Tie message
    }
    
    // Total votes row
    contentHeight += 40;
    
    // Table header + rows (each row 36px)
    contentHeight += 36;  // Header
    contentHeight += vd->candidate_count * 38;
    
    // Back button
    contentHeight += 50;
    
    // Calculate center Y position
    float centerStartY = (BASE_H - contentHeight) / 2.f;
    float currentY = centerStartY;
    
    // WINNER SECTION - Centered
    float bw = 600;
    float bx = (BASE_W - bw) / 2.f;
    
    if(vd->total_votes > 0){
        if(tie){
            draw_rect(bx, currentY, bw, 56, COL(80,60,0,255), C_GOLD, 2);
            draw_text("TIE - No single winner", BASE_W/2.f, currentY + 18, 20, C_GOLD, 1, BASE_W/2.f);
            currentY += 70;
        } else if(wi >= 0){
            draw_rect(bx, currentY, bw, 60, C_ACCENT, C_ACCENT2, 2);
            draw_rect(bx, currentY, 4, 48, C_GOLD, C_TRANS, 0);
            
            char wl[128];
            snprintf(wl, sizeof(wl), "WINNER: %s", vd->candidate_names[wi]);
            draw_text(wl, BASE_W/2.f, currentY + 18, 20, C_BLACK, 1, BASE_W/2.f);
            
            char vl[64];
            snprintf(vl, sizeof(vl), "%d votes (%.1f%%)", wv, vd->total_votes > 0 ? (float)wv / vd->total_votes * 100.f : 0.f);
            draw_text(vl, BASE_W/2.f, currentY + 38, 14, C_ACCENT, 1, BASE_W/2.f);
            
            currentY += 68;
        }
    }
    
    // Total Votes row
    char tot[64];
    snprintf(tot, sizeof(tot), "Total Votes: %d", vd->total_votes);
    draw_rect(bx, currentY, bw, 30, C_PANEL, C_BORDER, 1);
    draw_text(tot, BASE_W/2.f, currentY + 8, 14, C_ACCENT2, 1, BASE_W/2.f);
    currentY += 36;
    
    // Table header
    draw_rect(bx, currentY, bw, 28, C_ACCENT, C_TRANS, 0);
    draw_text("#", bx + 20, currentY + 7, 13, C_BLACK, 0, 0);
    draw_text("Candidate", bx + 60, currentY + 7, 13, C_BLACK, 0, 0);
    draw_text("Votes", bx + 400, currentY + 7, 13, C_BLACK, 0, 0);
    draw_text("%", bx + 500, currentY + 7, 13, C_BLACK, 0, 0);
    currentY += 34;
    
    // Table rows
    for(int i = 0; i < vd->candidate_count && currentY < BASE_H - 80; i++){
        float pct = vd->total_votes > 0 ? (float)vd->votes[i] / vd->total_votes * 100.f : 0.f;
        int iw = (wi == i && !tie);
        
        // Alternate row colors
        sfColor rowColor = iw ? COL(20,70,30,255) : ((i % 2 == 0) ? C_CARD : C_PANEL);
        draw_rect(bx, currentY, bw, 36, rowColor, iw ? C_SUCCESS : C_BORDER, iw ? 2.f : 1.f);
        if(iw) draw_rect(bx, currentY, 4, 36, C_GOLD, C_TRANS, 0);
        
        char num[8];
        snprintf(num, sizeof(num), "%d", i+1);
        draw_text(num, bx + 20, currentY + 10, 13, C_WHITE, 0, 0);
        
        draw_text(vd->candidate_names[i], bx + 60, currentY + 10, 13, iw ? C_ACCENT : C_WHITE, 0, 0);
        
        draw_progress_bar(bx + 310, currentY + 11, 80, 14, pct / 100.f, iw ? C_ACCENT2 : C_ACCENT);
        
        char vc[16];
        snprintf(vc, sizeof(vc), "%d", vd->votes[i]);
        draw_text(vc, bx + 400, currentY + 10, 13, C_WHITE, 0, 0);
        
        char pc[16];
        snprintf(pc, sizeof(pc), "%.1f%%", pct);
        draw_text(pc, bx + 505, currentY + 10, 13, iw ? C_ACCENT : C_ACCENT2, 0, 0);
        
        currentY += 42;
    }
    
    // Back button (centered at bottom)
    float btnY = currentY + 20;
    if(btnY > BASE_H - 80) btnY = BASE_H - 80;
    draw_btn("Back", BASE_W/2.f - 55, btnY, 110, 36, C_DGRAY, C_WHITE, 1);
    
    reader_exit();
    draw_footer("ESC or Back to return");
}
void render_perf(void){
    draw_rect(0,0,BASE_W,BASE_H,C_BG,C_TRANS,0);
    draw_header("PERFORMANCE COMPARISON","Auto-mode thread vs process timing");
    float cw=BASE_W-120; draw_card(60,82,cw,BASE_H-140,C_CARD,C_BORDER); draw_rect(60,82,cw,3,C_ACCENT,C_TRANS,0);
    FILE *pf=fopen("performance_data.txt","r"); memset(perf_lines,0,sizeof(perf_lines));
    if(pf){fread(perf_lines,1,sizeof(perf_lines)-1,pf);fclose(pf);}
    else strncpy(perf_lines,"No performance data yet. Run Auto Mode first.",sizeof(perf_lines)-1);
    float y=96; char *line=strtok(perf_lines,"\n"); int row=0;
    while(line&&y<BASE_H-100){
        sfColor tc=(row==0)?C_ACCENT2:(strstr(line,"Thread")?C_WHITE:strstr(line,"Process")?C_ACCENT2:C_WHITE);
        draw_text(line,80,y,13,tc,0,0); y+=22; row++; line=strtok(NULL,"\n");
    }
    draw_btn("Back",BASE_W/2.f-55,BASE_H-78,110,36,C_DGRAY,C_WHITE,1);
    draw_footer("ESC or Back to return");
}
void render_msg(void){
    switch(msg_ret){case SCR_MAIN:render_main();break;case SCR_ADMIN_PANEL:render_admin_panel();break;default:render_main();break;}
    float dw=500,dh=150,dx=(BASE_W-dw)/2.f,dy=(BASE_H-dh)/2.f;
    draw_rect(0,0,BASE_W,BASE_H,COL(0,0,0,100),C_TRANS,0);
    draw_card(dx,dy,dw,dh,C_PANEL,C_ACCENT); draw_rect(dx,dy,dw,3,C_ACCENT,C_TRANS,0);
    draw_text(msg_text,BASE_W/2.f,dy+30,15,C_WHITE,1,BASE_W/2.f);
    if(msg_timer<=0) draw_btn("OK",BASE_W/2.f-55,dy+dh-50,110,36,C_ACCENT,C_BLACK,1);
}
void render(void){
    if(cur_scr==SCR_SPLASH){sfRenderWindow_clear(win,C_BG);draw_splash();}
    else{
        sfRenderWindow_clear(win,C_BG);
        switch(cur_scr){
            case SCR_MAIN:render_main();break;
            case SCR_ADMIN_LOGIN:render_admin_login();break;
            case SCR_ADMIN_PANEL:render_admin_panel();break;
            case SCR_ADMIN_CANDIDATES:render_admin_candidates();break;
            case SCR_ADMIN_ADD_CAND:render_admin_add_cand();break;
            case SCR_ADMIN_LIVE:render_admin_live();break;
            case SCR_ADMIN_STATS:render_admin_stats();break;
            case SCR_ADMIN_RESET_CONFIRM:
            case SCR_ADMIN_DECLARE_CONFIRM:
            case SCR_ADMIN_EXPORT_CONFIRM:render_confirm();break;
            case SCR_ADMIN_PASSWORD_CONFIRM:render_password_confirm();break;
            case SCR_MANUAL_VOTER_ID:render_manual_vid();break;
            case SCR_MANUAL_VOTE:render_manual_vote();break;
            case SCR_MANUAL_SUCCESS:render_manual_success();break;
            case SCR_AUTO_SETUP_CITIES:render_auto_setup_cities();break;
            case SCR_AUTO_VOTER_COUNT:render_auto_voter_count();break;
            case SCR_AUTO_VOTER_DETAIL:render_auto_voter_detail();break;
            case SCR_AUTO_RUNNING:render_auto_running();break;
            case SCR_RESULTS:render_results();break;
            case SCR_PERF:render_perf();break;
            case SCR_MSG:render_msg();break;
            default:break;
        }
    }
    sfRenderWindow_display(win);
}
static int mouse_in(float mx,float my,float x,float y,float w,float h){return mx>=x&&mx<=x+w&&my>=y&&my<=y+h;}

void ev_splash(sfEvent *e) {
    if (splash_waiting_for_key) {
        if (e->type == sfEvtKeyPressed || e->type == sfEvtMouseButtonPressed) {
            go(SCR_MAIN);
        }
    }
    splash_timer += dt;
    if (splash_timer >= 1.5f) {
        splash_waiting_for_key = 1;
    }
}

void ev_main(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyUp) adm_sel = (adm_sel + 4) % 5;
        if (e->key.code == sfKeyDown) adm_sel = (adm_sel + 1) % 5;
        if (e->key.code == sfKeyEscape) sfRenderWindow_close(win);
        if (e->key.code == sfKeyReturn) {
            switch(adm_sel) {
                case 0:
                    if (!vd->voting_active) { go_msg("Voting has ended!", SCR_MAIN, 2.f); return; }
                    if (vd->candidate_count == 0) { go_msg("No candidates! Admin must add candidates first.", SCR_MAIN, 2.5f); return; }
                    go(SCR_MANUAL_VOTER_ID); break;
                case 1:
                    if (!vd->voting_active) { go_msg("Voting has ended!", SCR_MAIN, 2.f); return; }
                    if (vd->candidate_count == 0) { go_msg("No candidates! Admin must add candidates first.", SCR_MAIN, 2.5f); return; }
                    go(SCR_AUTO_SETUP_CITIES); break;
                case 2: go(SCR_RESULTS); break;
                case 3: go(SCR_PERF); break;
                case 4: go(SCR_ADMIN_LOGIN); break;
            }
        }
    }
    /* Mouse hover - highlight button under cursor */
    if (e->type == sfEvtMouseMoved) {
        float mx = (float)e->mouseMove.x / sx();
        float my = (float)e->mouseMove.y / sy();
        float bw = 340, bh = 52, gap = 14;
        float total_h = 5 * (bh + gap) - gap;
        float start_y = (BASE_H - total_h) / 2.f + 18;
        float bx = (BASE_W - bw) / 2.f;
        for (int i = 0; i < 5; i++) {
            float fy = start_y + i * (bh + gap);
            if (mx >= bx && mx <= bx + bw && my >= fy && my <= fy + bh) {
                adm_sel = i;
            }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        float bw = 340, bh = 52, gap = 14;
        float total_h = 5 * (bh + gap) - gap;
        float start_y = (BASE_H - total_h) / 2.f + 18;
        float bx = (BASE_W - bw) / 2.f;
        for (int i = 0; i < 5; i++) {
            float fy = start_y + i * (bh + gap);
            if (mx >= bx && mx <= bx + bw && my >= fy && my <= fy + bh) {
                adm_sel = i;
                sfEvent fake;
                fake.type = sfEvtKeyPressed;
                fake.key.code = sfKeyReturn;
                ev_main(&fake);
                return;
            }
        }
    }
}

void ev_admin_login(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyEscape) { go(SCR_MAIN); return; }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx=(float)e->mouseButton.x/sx(), my=(float)e->mouseButton.y/sy();
        if (mouse_in(mx,my,560,307,160,44)) { sfEvent f; f.type=sfEvtKeyPressed; f.key.code=sfKeyReturn; ev_admin_login(&f); return; }
    }
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyReturn) {
        pw_buf[pw_len] = '\0';
        if (strcmp(pw_buf, ADMIN_PASSWORD) == 0) {
            admin_tries = 3;
            go(SCR_ADMIN_PANEL);
        } else {
            admin_tries--;
            if (admin_tries <= 0) {
                go_msg("Too many failed attempts! Access locked.", SCR_MAIN, 3.f);
            } else {
                snprintf(pw_err, sizeof(pw_err), "Wrong password! %d attempt(s) left.", admin_tries);
                memset(pw_buf, 0, sizeof(pw_buf));
                pw_len = 0;
            }
        }
        return;
    }
    text_input(e, pw_buf, &pw_len, 63, false);
}

void ev_admin_panel(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyUp) adm_sel = (adm_sel + 6) % 7;
        if (k == sfKeyDown) adm_sel = (adm_sel + 1) % 7;
        if (k == sfKeyEscape) { go(SCR_MAIN); return; }
        if (k == sfKeyReturn) {
            switch(adm_sel) {
                case 0: go(SCR_ADMIN_LIVE); break;
                case 1: go(SCR_ADMIN_CANDIDATES); break;
                case 2:
                    go_password_confirm(1);
                    break;
                case 3:
                    do_export_report();
                    go_msg(msg_text, SCR_ADMIN_PANEL, 3.f);
                    break;
                case 4: go(SCR_ADMIN_STATS); break;
                case 5:
                    if (!vd->voting_active) { go_msg("Voting already ended!", SCR_ADMIN_PANEL, 2.f); return; }
                    go_password_confirm(2);
                    break;
                case 6: go(SCR_MAIN); break;
            }
        }
    }
    if (e->type == sfEvtMouseMoved) {
        float mx = (float)e->mouseMove.x / sx();
        float my = (float)e->mouseMove.y / sy();
        float bw = 480, bh = 48, gap = 10;
        float total = 7 * (bh + gap) - gap;
        float sy2 = (BASE_H - total) / 2.f + 5;
        float bx = (BASE_W - bw) / 2.f;
        for (int i = 0; i < 7; i++) {
            float fy = sy2 + i * (bh + gap);
            if (mx >= bx && mx <= bx + bw && my >= fy && my <= fy + bh) adm_sel = i;
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        float bw = 480, bh = 48, gap = 10;
        float total = 7 * (bh + gap) - gap;
        float sy2 = (BASE_H - total) / 2.f + 5;
        float bx = (BASE_W - bw) / 2.f;
        for (int i = 0; i < 7; i++) {
            float fy = sy2 + i * (bh + gap);
            if (mx >= bx && mx <= bx + bw && my >= fy && my <= fy + bh) {
                adm_sel = i;
                sfEvent fake;
                fake.type = sfEvtKeyPressed;
                fake.key.code = sfKeyReturn;
                ev_admin_panel(&fake);
                return;
            }
        }
    }
}

void ev_admin_candidates(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyEscape) { go(SCR_ADMIN_PANEL); return; }
        if (k == sfKeyLeft) cand_menu_sel = (cand_menu_sel + 2) % 3;
        if (k == sfKeyRight) cand_menu_sel = (cand_menu_sel + 1) % 3;
        if (k == sfKeyUp && vd->candidate_count > 0) cand_sel = (cand_sel + vd->candidate_count - 1) % vd->candidate_count;
        if (k == sfKeyDown && vd->candidate_count > 0) cand_sel = (cand_sel + 1) % vd->candidate_count;
        if (k == sfKeyReturn) {
            if (cand_menu_sel == 0) {
                go(SCR_ADMIN_ADD_CAND);
            } else if (cand_menu_sel == 1) {
                if (vd->candidate_count == 0) { go_msg("No candidates to delete!", SCR_ADMIN_CANDIDATES, 2.f); return; }
                if (vd->candidate_count <= 1) { go_msg("Cannot delete the last candidate!", SCR_ADMIN_CANDIDATES, 2.f); return; }
                del_idx = cand_sel;
                char body[128];
                snprintf(body, sizeof(body), "Delete candidate:\n\"%s\"?", vd->candidate_names[del_idx]);
                go_confirm("DELETE CANDIDATE", body, (Screen)96, SCR_ADMIN_CANDIDATES);
            } else if (cand_menu_sel == 2) {
                go(SCR_ADMIN_PANEL);
            }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        float by = BASE_H - 78;
        float bw = 140, bh = 42, gap = 18;
        float total_w = 3*bw + 2*gap;
        float start_x = (BASE_W - total_w)/2.f;
        if (mx >= start_x && mx <= start_x + bw && my >= by && my <= by + bh) {
            cand_menu_sel = 0;
            go(SCR_ADMIN_ADD_CAND);
            return;
        }
        if (mx >= start_x + bw + gap && mx <= start_x + 2*bw + gap && my >= by && my <= by + bh) {
            cand_menu_sel = 1;
            if (vd->candidate_count == 0) { go_msg("No candidates to delete!", SCR_ADMIN_CANDIDATES, 2.f); return; }
            if (vd->candidate_count <= 1) { go_msg("Cannot delete the last candidate!", SCR_ADMIN_CANDIDATES, 2.f); return; }
            del_idx = cand_sel;
            char body[128];
            snprintf(body, sizeof(body), "Delete candidate:\n\"%s\"?", vd->candidate_names[del_idx]);
            go_confirm("DELETE CANDIDATE", body, (Screen)96, SCR_ADMIN_CANDIDATES);
            return;
        }
        if (mx >= start_x + 2*(bw+gap) && mx <= start_x + 3*bw + 2*gap && my >= by && my <= by + bh) {
            cand_menu_sel = 2;
            go(SCR_ADMIN_PANEL);
            return;
        }
        float x0=80, y0=100, row=46;
        for (int i = 0; i < vd->candidate_count; i++) {
            float ry = y0 + 34 + i * row;
            if (mx >= x0 && mx <= x0 + BASE_W - 160 && my >= ry && my <= ry + row - 4) {
                cand_sel = i;
                return;
            }
        }
    }
}

void ev_admin_add_cand(sfEvent *e) {
    text_input(e, new_cand, &new_cand_len, MAX_NAME_LENGTH - 1, false);
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyLeft) add_cand_sel = 0;
        if (e->key.code == sfKeyRight) add_cand_sel = 1;
        if (e->key.code == sfKeyEscape) { go(SCR_ADMIN_CANDIDATES); add_cand_sel = 0; return; }
        if (e->key.code == sfKeyReturn) {
            if (add_cand_sel == 0) {
                new_cand[new_cand_len] = '\0';
                if (new_cand_len == 0) return;
                if (vd->candidate_count >= MAX_CANDIDATES) {
                    go_msg("Maximum candidates reached!", SCR_ADMIN_CANDIDATES, 2.f);
                    return;
                }
                if (is_duplicate_candidate(new_cand)) {
                    go_msg("Candidate with this name already exists!", SCR_ADMIN_ADD_CAND, 2.f);
                    memset(new_cand, 0, sizeof(new_cand));
                    new_cand_len = 0;
                    return;
                }
                writer_enter();
                strncpy(vd->candidate_names[vd->candidate_count], new_cand, MAX_NAME_LENGTH - 1);
                vd->candidate_names[vd->candidate_count][MAX_NAME_LENGTH - 1] = '\0';
                vd->votes[vd->candidate_count] = 0;
                vd->candidate_count++;
                writer_exit();
                go_msg("Candidate added successfully!", SCR_ADMIN_CANDIDATES, 1.5f);
                add_cand_sel = 0;
            } else {
                go(SCR_ADMIN_CANDIDATES);
                add_cand_sel = 0;
            }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (mouse_in(mx,my,495,315,130,42)) {
            add_cand_sel = 0;
            new_cand[new_cand_len] = '\0';
            if (new_cand_len == 0) return;
            if (vd->candidate_count >= MAX_CANDIDATES) {
                go_msg("Maximum candidates reached!", SCR_ADMIN_CANDIDATES, 2.f);
                return;
            }
            if (is_duplicate_candidate(new_cand)) {
                go_msg("Candidate with this name already exists!", SCR_ADMIN_ADD_CAND, 2.f);
                memset(new_cand, 0, sizeof(new_cand));
                new_cand_len = 0;
                return;
            }
            writer_enter();
            strncpy(vd->candidate_names[vd->candidate_count], new_cand, MAX_NAME_LENGTH - 1);
            vd->candidate_names[vd->candidate_count][MAX_NAME_LENGTH - 1] = '\0';
            vd->votes[vd->candidate_count] = 0;
            vd->candidate_count++;
            writer_exit();
            go_msg("Candidate added successfully!", SCR_ADMIN_CANDIDATES, 1.5f);
            add_cand_sel = 0;
            return;
        }
        if (mouse_in(mx,my,655,315,130,42)) {
            add_cand_sel = 1;
            go(SCR_ADMIN_CANDIDATES);
            add_cand_sel = 0;
            return;
        }
    }
}

void ev_admin_live(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && (e->key.code == sfKeyEscape || e->key.code == sfKeyQ))
        go(SCR_ADMIN_PANEL);
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (mouse_in(mx,my,BASE_W/2.f-55,BASE_H-78,110,36)) {
            go(SCR_ADMIN_PANEL);
        }
    }
}

void ev_admin_stats(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyEscape) go(SCR_ADMIN_PANEL);
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (mouse_in(mx,my,BASE_W/2.f-55,BASE_H-78,110,36)) {
            go(SCR_ADMIN_PANEL);
        }
    }
}

void ev_confirm(sfEvent *e) {
    bool confirmed = false;
    bool cancelled = false;
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyLeft || e->key.code == sfKeyRight) {
            confirm_selection = !confirm_selection;
            return;
        }
        else if (e->key.code == sfKeyY) confirmed = true;
        else if (e->key.code == sfKeyN) cancelled = true;
        else if (e->key.code == sfKeyReturn) {
            if (confirm_selection == 0) confirmed = true;
            else cancelled = true;
        }
        else if (e->key.code == sfKeyEscape) cancelled = true;
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        /* YES button: bx=450..630, by=415..459 */
        if (mx >= 450 && mx <= 630 && my >= 415 && my <= 459) confirmed = true;
        /* NO button: bx=650..830, by=415..459 */
        else if (mx >= 650 && mx <= 830 && my >= 415 && my <= 459) cancelled = true;
    }
    if (cancelled) { go(confirm_no); confirm_selection = 0; return; }
    if (confirmed) {
        int action = (int)confirm_yes;
        if (action == 96) {
            if (del_idx >= 0 && del_idx < vd->candidate_count && vd->candidate_count > 1) {
                writer_enter();
                for (int i = del_idx; i < vd->candidate_count - 1; i++) {
                    strncpy(vd->candidate_names[i], vd->candidate_names[i + 1], MAX_NAME_LENGTH);
                    vd->candidate_names[i][MAX_NAME_LENGTH-1] = '\0';
                    vd->votes[i] = vd->votes[i + 1];
                }
                vd->candidate_count--;
                writer_exit();
            }
            del_idx = -1;
            go_msg("Candidate deleted.", SCR_ADMIN_CANDIDATES, 1.5f);
        } else {
            go(confirm_yes);
        }
        confirm_selection = 0;
    }
}

void ev_password_confirm(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyEscape) { go(SCR_ADMIN_PANEL); return; }
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyReturn) {
        pw_buf[pw_len] = '\0';
        if (strcmp(pw_buf, ADMIN_PASSWORD) == 0) {
            if (pending_action == 1) {
                writer_enter();
                memset(vd->votes, 0, sizeof(vd->votes));
                memset(vd->voted_ids, 0, sizeof(vd->voted_ids));
                vd->total_votes = 0;
                vd->voted_count = 0;
                vd->voters_completed = 0;
                memset(vd->last_activity, 0, sizeof(vd->last_activity));
                vd->activity_count = 0;
                vd->start_time = 0;
                vd->voting_active = 1;
                vd->results_visible = 0;
                writer_exit();
                go_msg("Election reset! Voting is now active.", SCR_ADMIN_PANEL, 2.f);
            } else if (pending_action == 2) {
                writer_enter();
                vd->voting_active = 0;
                vd->results_visible = 1;
                writer_exit();
                go_msg("Results are declared! Voting has ended.\nResults are now visible to all.", SCR_ADMIN_PANEL, 3.f);
            }
            pending_action = 0;
        } else {
            admin_tries--;
            if (admin_tries <= 0) {
                go_msg("Too many failed attempts! Action cancelled.", SCR_ADMIN_PANEL, 2.f);
                pending_action = 0;
            } else {
                snprintf(pw_err, sizeof(pw_err), "Wrong password! %d attempt(s) left.", admin_tries);
                memset(pw_buf, 0, sizeof(pw_buf));
                pw_len = 0;
            }
        }
        return;
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx=(float)e->mouseButton.x/sx(), my=(float)e->mouseButton.y/sy();
        if (mouse_in(mx,my,550,348,180,42)) { sfEvent f; f.type=sfEvtKeyPressed; f.key.code=sfKeyReturn; ev_password_confirm(&f); return; }
    }
    text_input(e, pw_buf, &pw_len, 63, false);
}

void ev_manual_vid(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyEscape) { go(SCR_MAIN); return; }
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyUp) manual_field_sel = (manual_field_sel + 1) % 2;
        if (e->key.code == sfKeyDown) manual_field_sel = (manual_field_sel + 1) % 2;
    }
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyReturn) {
        if (manual_field_sel == 0) {
            manual_field_sel = 1;
            return;
        } else {
            vid_buf[vid_len] = '\0';
            manual_city[manual_city_len] = '\0';
            vid_val = atoi(vid_buf);
            if (!validate_vid(vid_val)) {
                snprintf(vid_err, sizeof(vid_err), "Invalid ID! Must be between 1000 and 9999.");
                memset(vid_buf, 0, sizeof(vid_buf));
                vid_len = 0;
                return;
            }
            if (manual_city_len == 0) {
                snprintf(vid_err, sizeof(vid_err), "City cannot be empty!");
                return;
            }
            reader_enter();
            int dup = 0;
            for (int i = 0; i < vd->voted_count; i++)
                if (vd->voted_ids[i] == vid_val) { dup = 1; break; }
            reader_exit();
            if (dup) {
                snprintf(vid_err, sizeof(vid_err), "Voter ID %d has already voted!", vid_val);
                memset(vid_buf, 0, sizeof(vid_buf));
                vid_len = 0;
                return;
            }
            go(SCR_MANUAL_VOTE);
            return;
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (mx >= 400 && mx <= 880 && my >= 150 && my <= 194) manual_field_sel = 0;
        if (mx >= 400 && mx <= 880 && my >= 240 && my <= 284) manual_field_sel = 1;
        if (mx >= BASE_W/2.f - 75 && mx <= BASE_W/2.f + 75 && my >= 368 && my <= 410) {
            vid_buf[vid_len] = '\0';
            manual_city[manual_city_len] = '\0';
            vid_val = atoi(vid_buf);
            if (!validate_vid(vid_val)) {
                snprintf(vid_err, sizeof(vid_err), "Invalid ID! Must be between 1000 and 9999.");
                memset(vid_buf, 0, sizeof(vid_buf));
                vid_len = 0;
                return;
            }
            if (manual_city_len == 0) {
                snprintf(vid_err, sizeof(vid_err), "City cannot be empty!");
                return;
            }
            reader_enter();
            int dup = 0;
            for (int i = 0; i < vd->voted_count; i++)
                if (vd->voted_ids[i] == vid_val) { dup = 1; break; }
            reader_exit();
            if (dup) {
                snprintf(vid_err, sizeof(vid_err), "Voter ID %d has already voted!", vid_val);
                memset(vid_buf, 0, sizeof(vid_buf));
                vid_len = 0;
                return;
            }
            go(SCR_MANUAL_VOTE);
            return;
        }
    }
    if (manual_field_sel == 0) text_input(e, vid_buf, &vid_len, 10, true);
    else text_input(e, manual_city, &manual_city_len, 48, false);
}

void ev_manual_vote(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyEscape) {
            if (confirm_vote_show) confirm_vote_show = 0;
            else go(SCR_MANUAL_VOTER_ID);
            return;
        }
        if (!confirm_vote_show) {
            if (k == sfKeyUp && vd->candidate_count > 0) vote_sel = (vote_sel + vd->candidate_count - 1) % vd->candidate_count;
            if (k == sfKeyDown && vd->candidate_count > 0) vote_sel = (vote_sel + 1) % vd->candidate_count;
            if (k == sfKeyReturn) confirm_vote_show = 1;
        } else {
            if (k == sfKeyReturn) {
                create_log_file("Manual");
                int res = cast_vote(vid_val, vote_sel);
                if (res == 1) {
                    voted_candidate = vote_sel;
                    log_manual_city_safe(vid_val, manual_city);
                    confirm_vote_show = 0;
                    go(SCR_MANUAL_SUCCESS);
                } else if (res == -1) snprintf(vote_err, sizeof(vote_err), "Voting has ended!");
                else if (res == -3) snprintf(vote_err, sizeof(vote_err), "Already voted!");
                else snprintf(vote_err, sizeof(vote_err), "Vote failed! Please try again.");
                confirm_vote_show = 0;
            }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (!confirm_vote_show) {
            if (mx >= BASE_W/2.f - 55 && mx <= BASE_W/2.f + 55 && my >= BASE_H - 52 && my <= BASE_H - 16) {
                go(SCR_MANUAL_VOTER_ID);
                return;
            }
            float bw=500, bh=50, gap=10;
            float total=vd->candidate_count*(bh+gap)-gap;
            float start_y=(BASE_H-total)/2.f + 28;
            float bx=(BASE_W-bw)/2.f;
            for (int i = 0; i < vd->candidate_count; i++) {
                float fy = start_y + i * (bh + gap);
                if (mx >= bx && mx <= bx + bw && my >= fy && my <= fy + bh) {
                    vote_sel = i;
                    confirm_vote_show = 1;
                    return;
                }
            }
        } else {
            /* CONFIRM: x=410..620, y=407..453 */
            if (mx >= 410 && mx <= 620 && my >= 407 && my <= 453) {
                int res = cast_vote(vid_val, vote_sel);
                if (res == 0) { voted_candidate = vote_sel; go(SCR_MANUAL_SUCCESS); }
                else if (res == 1) { snprintf(vote_err, sizeof(vote_err), "Already voted!"); confirm_vote_show=0; }
                else { snprintf(vote_err, sizeof(vote_err), "Voting not active."); confirm_vote_show=0; }
                return;
            }
            /* CANCEL: x=660..870, y=407..453 */
            if (mx >= 660 && mx <= 870 && my >= 407 && my <= 453) {
                confirm_vote_show = 0;
                return;
            }
        }
    }
}

void ev_auto_setup_cities(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyEscape) { go(SCR_MAIN); return; }
        if (k == sfKeyUp) auto_field = (auto_field + 1) % 2;
        if (k == sfKeyDown) auto_field = (auto_field + 1) % 2;
        if (k == sfKeyReturn) {
            if (auto_field == 0) { auto_field = 1; return; }
            if (strlen(city1) > 0 && strlen(city2) > 0) {
                char c1[50], c2[50];
                strncpy(c1, city1, 49); c1[49]='\0';
                strncpy(c2, city2, 49); c2[49]='\0';
                for (int i = 0; c1[i]; i++) c1[i] = tolower((unsigned char)c1[i]);
                for (int i = 0; c2[i]; i++) c2[i] = tolower((unsigned char)c2[i]);
                auto_use_threads = (strcmp(c1, c2) == 0);
                go(SCR_AUTO_VOTER_COUNT);
            }
        }
    }
    if (e->type == sfEvtTextEntered) {
        sfUint32 c = e->text.unicode;
        if (auto_field == 0) {
            if (c == 8 && city1_len > 0) { city1[--city1_len] = '\0'; }
            else if (c >= 32 && c < 127 && city1_len < 48) { city1[city1_len++] = (char)c; city1[city1_len] = '\0'; }
        } else {
            if (c == 8 && city2_len > 0) { city2[--city2_len] = '\0'; }
            else if (c >= 32 && c < 127 && city2_len < 48) { city2[city2_len++] = (char)c; city2[city2_len] = '\0'; }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx=(float)e->mouseButton.x/sx(), my=(float)e->mouseButton.y/sy();
        if (my >= 112 && my <= 176) auto_field = 0;
        else if (my >= 202 && my <= 266) auto_field = 1;
        /* NEXT btn: x=565,y=373,w=150,h=44 */
        if (mouse_in(mx,my,565,373,150,44)) {
            sfEvent f; f.type=sfEvtKeyPressed; f.key.code=sfKeyReturn;
            auto_field=1; ev_auto_setup_cities(&f);
        }
    }
}

void ev_auto_voter_count(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyEscape) { go(SCR_AUTO_SETUP_CITIES); return; }
        if (k == sfKeyReturn) {
            auto_n = atoi(nv_buf);
            if (auto_n >= 1 && auto_n <= MAX_VOTERS) {
                go(SCR_AUTO_VOTER_DETAIL);
            } else {
                snprintf(auto_setup_err, sizeof(auto_setup_err), "Number must be 1–%d!", MAX_VOTERS);
            }
        }
    }
    if (e->type == sfEvtTextEntered) {
        sfUint32 c = e->text.unicode;
        if (c == 8 && nv_len > 0) { nv_buf[--nv_len] = ' '; }
        else if (c >= '0' && c <= '9' && nv_len < 6) { nv_buf[nv_len++] = (char)c; nv_buf[nv_len] = ' '; }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx=(float)e->mouseButton.x/sx(), my=(float)e->mouseButton.y/sy();
        if (mouse_in(mx,my,565,336,150,44)) {
            sfEvent f; f.type=sfEvtKeyPressed; f.key.code=sfKeyReturn;
            ev_auto_voter_count(&f);
        }
    }
}

void ev_auto_voter_detail(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        sfKeyCode k = e->key.code;
        if (k == sfKeyEscape) { go(SCR_AUTO_VOTER_COUNT); return; }
        if (k == sfKeyUp && vd->candidate_count > 0) acand_sel = (acand_sel + vd->candidate_count - 1) % vd->candidate_count;
        if (k == sfKeyDown && vd->candidate_count > 0) acand_sel = (acand_sel + 1) % vd->candidate_count;
        if (k == sfKeyReturn) {
            avid_buf[avid_len] = '\0';
            int vid = atoi(avid_buf);
            if (!validate_vid(vid)) {
                snprintf(avid_err, sizeof(avid_err), "Invalid Voter ID! Must be 1000 and 9999.");
                return;
            }
            for (int i = 0; i < auto_cur; i++) {
                if (auto_entries[i].vid == vid) {
                    snprintf(avid_err, sizeof(avid_err), "Duplicate Voter ID in this batch!");
                    return;
                }
            }
            auto_entries[auto_cur].vid = vid;
            auto_entries[auto_cur].cid = acand_sel;
            auto_cur++;
            if (auto_cur >= auto_n) {
                go(SCR_AUTO_RUNNING);
            } else {
                memset(avid_buf, 0, sizeof(avid_buf));
                avid_len = 0;
                acand_sel = 0;
                memset(avid_err, 0, sizeof(avid_err));
            }
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx=(float)e->mouseButton.x/sx(), my=(float)e->mouseButton.y/sy();
        /* candidate rows: cx+30=400, cy+132=230, each 32px tall */
        for (int i = 0; i < vd->candidate_count && i < 6; i++) {
            float fy = 230 + i * 32;
            if (mouse_in(mx,my,400,fy,480,28)) { acand_sel = i; return; }
        }
        /* Next Voter / Start Voting btn */
        if (mouse_in(mx,my,550,632,180,44)) {
            sfEvent f; f.type=sfEvtKeyPressed; f.key.code=sfKeyReturn;
            ev_auto_voter_detail(&f);
        }
    }
    text_input(e, avid_buf, &avid_len, 10, true);
}

void ev_auto_running(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyLeft) auto_complete_sel = 0;
        if (e->key.code == sfKeyRight) auto_complete_sel = 1;
        if (e->key.code == sfKeyEscape && auto_done_flag) go(SCR_MAIN);
        if (e->key.code == sfKeyReturn && auto_done_flag) {
            if (auto_complete_sel == 0) go(SCR_MANUAL_VOTER_ID);
            else go(SCR_MAIN);
        }
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft && auto_done_flag) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        float btn_w = 190, gap = 25;
        float start_x = (BASE_W - (2*btn_w + gap)) / 2.f;
        float bby = 128 + 195;
        if (mx >= start_x && mx <= start_x + btn_w && my >= bby && my <= bby+42) go(SCR_MANUAL_VOTER_ID);
        else if (mx >= start_x + btn_w + gap && mx <= start_x + 2*btn_w + gap && my >= bby && my <= bby+42) go(SCR_MAIN);
    }
}

void ev_results(sfEvent *e) {
    if (e->type == sfEvtKeyPressed) {
        if (e->key.code == sfKeyEscape) go(SCR_MAIN);
        if (e->key.code == sfKeyReturn && results_back_sel == 0) go(SCR_MAIN);
    }
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        /* Results locked Back btn at y=308, normal Back at BASE_H-78 */
        if (mouse_in(mx,my,BASE_W/2.f-55,308,110,36)) { go(SCR_MAIN); return; }
        if (mouse_in(mx,my,BASE_W/2.f-55,BASE_H-78,110,36)) { go(SCR_MAIN); return; }
    }
}

void ev_perf(sfEvent *e) {
    if (e->type == sfEvtKeyPressed && e->key.code == sfKeyEscape) go(SCR_MAIN);
    if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
        float mx = (float)e->mouseButton.x / sx();
        float my = (float)e->mouseButton.y / sy();
        if (mx >= BASE_W/2.f - 55 && mx <= BASE_W/2.f + 55 && my >= BASE_H - 78 && my <= BASE_H - 42) {
            go(SCR_MAIN);
        }
    }
}

void ev_msg(sfEvent *e) {
    if (e->type == sfEvtKeyPressed || (e->type == sfEvtMouseButtonPressed)) {
        cur_scr = msg_ret;
    }
}

void events(sfEvent *e) {
    if (cur_scr == SCR_SPLASH) {
        ev_splash(e);
        return;
    }
    switch (cur_scr) {
        case SCR_MAIN:                    ev_main(e); break;
        case SCR_ADMIN_LOGIN:             ev_admin_login(e); break;
        case SCR_ADMIN_PANEL:             ev_admin_panel(e); break;
        case SCR_ADMIN_CANDIDATES:        ev_admin_candidates(e); break;
        case SCR_ADMIN_ADD_CAND:          ev_admin_add_cand(e); break;
        case SCR_ADMIN_LIVE:              ev_admin_live(e); break;
        case SCR_ADMIN_STATS:             ev_admin_stats(e); break;
        case SCR_ADMIN_RESET_CONFIRM:
        case SCR_ADMIN_DECLARE_CONFIRM:
        case SCR_ADMIN_EXPORT_CONFIRM:    ev_confirm(e); break;
        case SCR_ADMIN_PASSWORD_CONFIRM:  ev_password_confirm(e); break;
        case SCR_MANUAL_VOTER_ID:         ev_manual_vid(e); break;
        case SCR_MANUAL_VOTE:             ev_manual_vote(e); break;
        case SCR_MANUAL_SUCCESS:
            if (e->type == sfEvtKeyPressed) {
                if (e->key.code == sfKeyLeft) manual_success_sel = 0;
                if (e->key.code == sfKeyRight) manual_success_sel = 1;
                if (e->key.code == sfKeyReturn) {
                    if (manual_success_sel == 0) go(SCR_MANUAL_VOTER_ID);
                    else go(SCR_MAIN);
                }
                if (e->key.code == sfKeyEscape) go(SCR_MAIN);
            }
            if (e->type == sfEvtMouseButtonPressed && e->mouseButton.button == sfMouseLeft) {
                float mx = (float)e->mouseButton.x / sx();
                float my = (float)e->mouseButton.y / sy();
                float btn_w = 190, gap = 25;
                float start_x = (BASE_W - (2*btn_w + gap)) / 2.f;
                if (mx >= start_x && mx <= start_x + btn_w && my >= 291 && my <= 333) go(SCR_MANUAL_VOTER_ID);
                if (mx >= start_x + btn_w + gap && mx <= start_x + 2*btn_w + gap && my >= 291 && my <= 333) go(SCR_MAIN);
            }
            break;
        case SCR_AUTO_SETUP_CITIES:       ev_auto_setup_cities(e); break;
        case SCR_AUTO_VOTER_COUNT:        ev_auto_voter_count(e); break;
        case SCR_AUTO_VOTER_DETAIL:       ev_auto_voter_detail(e); break;
        case SCR_AUTO_RUNNING:            ev_auto_running(e); break;
        case SCR_RESULTS:                 ev_results(e); break;
        case SCR_PERF:                    ev_perf(e); break;
        case SCR_MSG:                     ev_msg(e); break;
        default: break;
    }
}

/* ==================== MAIN ==================== */
int main(void) {
    umask(0077);
    backend_init();
    init_perf_file();
    init_worker_pool();          /* NEW: start worker pool (4 threads) */
    init_fifo();                 /* NEW: start named pipe listener */
    atexit(cleanup_on_exit);
   

    sfVideoMode vm = {BASE_W, BASE_H, 32};
    win = sfRenderWindow_create(vm, "Synchronized Voting System",
                                sfClose, NULL);
    if (!win) { fprintf(stderr, "Cannot create window\n"); backend_cleanup(0); return 1; }
    sfRenderWindow_setFramerateLimit(win, 60);

    fnt = sfFont_createFromFile(FONT_PATH);
    if (!fnt) {
        fprintf(stderr, "Cannot load font: %s\n", FONT_PATH);
        fprintf(stderr, "Run: sudo apt install fonts-dejavu-core\n");
        sfRenderWindow_destroy(win);
        backend_cleanup(0);
        return 1;
    }

    clk = sfClock_create();
    pulseClock = sfClock_create(); 
    cur_scr = SCR_SPLASH;
    splash_timer = 0.f;
    splash_waiting_for_key = 0;
    adm_sel = 0;

    while (sfRenderWindow_isOpen(win) && !cleanup_flag) {
        sfTime t = sfClock_restart(clk);
        dt = t.microseconds / 1000000.f;

        if (cur_scr == SCR_SPLASH) splash_timer += dt;
        if (cur_scr == SCR_MSG && msg_timer > 0) {
            msg_timer -= dt;
            if (msg_timer <= 0) cur_scr = msg_ret;
        }
        if (cur_scr == SCR_AUTO_RUNNING) auto_update();

        sfEvent e;
        while (sfRenderWindow_pollEvent(win, &e)) {
            if (e.type == sfEvtClosed) sfRenderWindow_close(win);
            else events(&e);
        }
        render();
    }
sfClock_destroy(pulseClock);
    sfClock_destroy(clk);
    sfFont_destroy(fnt);
    sfRenderWindow_destroy(win);
    printf("\nThank you for using the Voting System!\n");
    return 0;
} 
