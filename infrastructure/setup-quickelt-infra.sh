#!/bin/bash

# =============================================================================
# QUICKELT INFRASTRUCTURE SETUP SCRIPT / SCRIPT DE CONFIGURAÇÃO QUICKELT
# =============================================================================
# Este script configura automaticamente toda a infraestrutura necessária
# para o projeto QuickELT em ambiente Ubuntu Linux.
#
# This script automatically sets up all necessary infrastructure
# for the QuickELT project on Ubuntu Linux environment.
#
# PREREQUISITOS / PREREQUISITES:
# - Ubuntu 18.04+ ou Debian 10+
# - Usuário com privilégios sudo
# - Conexão com internet
#
# COMO USAR / HOW TO USE:
# chmod +x setup-quickelt-infra.sh
# ./setup-quickelt-infra.sh
# =============================================================================

set -e  # Parar execução em caso de erro / Stop execution on error

# Cores para output / Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para logging / Logging function
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "\n${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}\n"
}

# Função para verificar se comando existe / Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Função para verificar sistema operacional / Check operating system
check_os() {
    log_step "🔍 VERIFICANDO SISTEMA OPERACIONAL / CHECKING OPERATING SYSTEM"
    
    if [[ "$OSTYPE" != "linux-gnu"* ]]; then
        log_error "Este script é destinado apenas para Linux. Sistema detectado: $OSTYPE"
        log_error "This script is for Linux only. Detected system: $OSTYPE"
        exit 1
    fi
    
    if ! command_exists lsb_release; then
        log_warn "lsb_release não encontrado. Instalando..."
        log_warn "lsb_release not found. Installing..."
        sudo apt update && sudo apt install -y lsb-release
    fi
    
    local OS_NAME=$(lsb_release -si)
    local OS_VERSION=$(lsb_release -sr)
    
    log_info "Sistema operacional: $OS_NAME $OS_VERSION"
    log_info "Operating system: $OS_NAME $OS_VERSION"
    
    if [[ "$OS_NAME" != "Ubuntu" ]] && [[ "$OS_NAME" != "Debian" ]]; then
        log_warn "Sistema não testado. Continue por sua conta e risco."
        log_warn "Untested system. Continue at your own risk."
        read -p "Deseja continuar? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# Atualizar sistema / Update system
update_system() {
    log_step "📦 ATUALIZANDO SISTEMA / UPDATING SYSTEM"
    
    log_info "Atualizando lista de pacotes..."
    log_info "Updating package list..."
    sudo apt update
    
    log_info "Atualizando pacotes instalados..."
    log_info "Upgrading installed packages..."
    sudo apt upgrade -y
    
    log_info "Instalando pacotes essenciais..."
    log_info "Installing essential packages..."
    sudo apt install -y \
        curl \
        wget \
        git \
        unzip \
        software-properties-common \
        apt-transport-https \
        ca-certificates \
        gnupg \
        lsb-release \
        build-essential \
        python3-dev \
        python3-pip \
        python3-venv
}

# Instalar Docker / Install Docker
install_docker() {
    log_step "🐳 INSTALANDO DOCKER / INSTALLING DOCKER"
    
    if command_exists docker; then
        log_info "Docker já está instalado: $(docker --version)"
        log_info "Docker is already installed: $(docker --version)"
    else
        log_info "Instalando Docker..."
        log_info "Installing Docker..."
        
        # Remover versões antigas / Remove old versions
        sudo apt remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true
        
        # Adicionar repositório oficial Docker / Add official Docker repository
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
        
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        
        sudo apt update
        sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        
        # Adicionar usuário ao grupo docker / Add user to docker group
        sudo usermod -aG docker $USER
        
        log_info "Docker instalado com sucesso!"
        log_info "Docker installed successfully!"
    fi
    
    # Verificar se Docker está rodando / Check if Docker is running
    if ! sudo systemctl is-active --quiet docker; then
        log_info "Iniciando serviço Docker..."
        log_info "Starting Docker service..."
        sudo systemctl start docker
        sudo systemctl enable docker
    fi
    
    log_info "Status do Docker: $(sudo systemctl is-active docker)"
    log_info "Docker status: $(sudo systemctl is-active docker)"
}

# Instalar Docker Compose / Install Docker Compose
install_docker_compose() {
    log_step "🔧 INSTALANDO DOCKER COMPOSE / INSTALLING DOCKER COMPOSE"
    
    if command_exists docker-compose; then
        log_info "Docker Compose já está instalado: $(docker-compose --version)"
        log_info "Docker Compose is already installed: $(docker-compose --version)"
    else
        log_info "Instalando Docker Compose..."
        log_info "Installing Docker Compose..."
        
        # Verificar se Docker Compose plugin está disponível / Check if Docker Compose plugin is available
        if docker compose version >/dev/null 2>&1; then
            log_info "Docker Compose Plugin detectado. Criando alias..."
            log_info "Docker Compose Plugin detected. Creating alias..."
            echo 'alias docker-compose="docker compose"' >> ~/.bashrc
        else
            # Instalar versão standalone / Install standalone version
            local COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -Po '"tag_name": "\K.*?(?=")')
            sudo curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
            sudo chmod +x /usr/local/bin/docker-compose
        fi
    fi
}

# Instalar Python e dependências / Install Python and dependencies
install_python() {
    log_step "🐍 CONFIGURANDO PYTHON / SETTING UP PYTHON"
    
    local PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2)
    log_info "Versão do Python: $PYTHON_VERSION"
    log_info "Python version: $PYTHON_VERSION"
    
    # Verificar se versão é adequada / Check if version is adequate
    if [[ $(echo "$PYTHON_VERSION" | cut -d'.' -f1) -lt 3 ]] || [[ $(echo "$PYTHON_VERSION" | cut -d'.' -f2) -lt 8 ]]; then
        log_warn "Python 3.8+ é recomendado. Versão atual: $PYTHON_VERSION"
        log_warn "Python 3.8+ is recommended. Current version: $PYTHON_VERSION"
    fi
    
    # Instalar pip se necessário / Install pip if needed
    if ! command_exists pip3; then
        log_info "Instalando pip..."
        log_info "Installing pip..."
        sudo apt install -y python3-pip
    fi
    
    # Atualizar pip / Update pip
    log_info "Atualizando pip..."
    log_info "Updating pip..."
    python3 -m pip install --upgrade pip
    
    # Instalar virtualenv / Install virtualenv
    log_info "Instalando virtualenv..."
    log_info "Installing virtualenv..."
    python3 -m pip install virtualenv
}

# Configurar estrutura do projeto / Set up project structure
setup_project_structure() {
    log_step "📁 CONFIGURANDO ESTRUTURA DO PROJETO / SETTING UP PROJECT STRUCTURE"
    
    local PROJECT_DIR="/opt/quickelt"
    
    log_info "Criando diretório do projeto: $PROJECT_DIR"
    log_info "Creating project directory: $PROJECT_DIR"
    
    sudo mkdir -p $PROJECT_DIR
    sudo chown $USER:$USER $PROJECT_DIR
    
    # Navegar para o diretório do projeto / Navigate to project directory
    cd $PROJECT_DIR
    
    # Se já existe um projeto, fazer backup / If project already exists, backup
    if [ -d ".git" ]; then
        log_warn "Projeto existente detectado. Fazendo backup..."
        log_warn "Existing project detected. Making backup..."
        sudo cp -r $PROJECT_DIR ${PROJECT_DIR}_backup_$(date +%Y%m%d_%H%M%S)
    fi
    
    # Clonar ou atualizar repositório / Clone or update repository
    if [ ! -d ".git" ]; then
        log_info "Clonando repositório QuickELT..."
        log_info "Cloning QuickELT repository..."
        git clone https://github.com/mpraes/quickelt.git .
    else
        log_info "Atualizando repositório existente..."
        log_info "Updating existing repository..."
        git pull origin main
    fi
    
    # Configurar permissões / Set permissions
    sudo chown -R $USER:$USER $PROJECT_DIR
    chmod +x infrastructure/setup-quickelt-infra.sh 2>/dev/null || true
}

# Configurar ambiente Python / Set up Python environment
setup_python_environment() {
    log_step "🔧 CONFIGURANDO AMBIENTE PYTHON / SETTING UP PYTHON ENVIRONMENT"
    
    cd /opt/quickelt
    
    # Criar ambiente virtual / Create virtual environment
    if [ ! -d ".venv" ]; then
        log_info "Criando ambiente virtual..."
        log_info "Creating virtual environment..."
        python3 -m venv .venv
    fi
    
    # Ativar ambiente virtual / Activate virtual environment
    log_info "Ativando ambiente virtual..."
    log_info "Activating virtual environment..."
    source .venv/bin/activate
    
    # Atualizar pip no ambiente virtual / Update pip in virtual environment
    log_info "Atualizando pip no ambiente virtual..."
    log_info "Updating pip in virtual environment..."
    python -m pip install --upgrade pip
    
    # Instalar dependências / Install dependencies
    if [ -f "requirements.txt" ]; then
        log_info "Instalando dependências do requirements.txt..."
        log_info "Installing dependencies from requirements.txt..."
        pip install -r requirements.txt
    else
        log_warn "requirements.txt não encontrado. Instalando dependências básicas..."
        log_warn "requirements.txt not found. Installing basic dependencies..."
        pip install pandas polars boto3 sqlalchemy pytest python-dotenv
    fi
}

# Configurar infraestrutura Docker / Set up Docker infrastructure
setup_docker_infrastructure() {
    log_step "🚀 CONFIGURANDO INFRAESTRUTURA DOCKER / SETTING UP DOCKER INFRASTRUCTURE"
    
    cd /opt/quickelt/infrastructure
    
    # Verificar se docker-compose.yml existe / Check if docker-compose.yml exists
    if [ ! -f "docker-compose.yml" ]; then
        log_error "docker-compose.yml não encontrado em infrastructure/"
        log_error "docker-compose.yml not found in infrastructure/"
        return 1
    fi
    
    # Criar diretório para scripts de inicialização / Create directory for init scripts
    mkdir -p init-scripts
    
    # Parar containers existentes / Stop existing containers
    log_info "Parando containers existentes..."
    log_info "Stopping existing containers..."
    docker-compose down 2>/dev/null || true
    
    # Subir infraestrutura / Start infrastructure
    log_info "Iniciando infraestrutura (PostgreSQL + MinIO)..."
    log_info "Starting infrastructure (PostgreSQL + MinIO)..."
    docker-compose up -d
    
    # Aguardar containers ficarem prontos / Wait for containers to be ready
    log_info "Aguardando containers ficarem prontos..."
    log_info "Waiting for containers to be ready..."
    sleep 30
    
    # Verificar status dos containers / Check container status
    log_info "Status dos containers:"
    log_info "Container status:"
    docker-compose ps
}

# Configurar arquivo .env / Set up .env file
setup_environment_file() {
    log_step "⚙️ CONFIGURANDO ARQUIVO .ENV / SETTING UP .ENV FILE"
    
    cd /opt/quickelt
    
    local ENV_FILE=".env"
    
    if [ -f "$ENV_FILE" ]; then
        log_warn "Arquivo .env existente encontrado. Fazendo backup..."
        log_warn "Existing .env file found. Making backup..."
        cp $ENV_FILE ${ENV_FILE}.backup.$(date +%Y%m%d_%H%M%S)
    fi
    
    log_info "Criando arquivo .env com configurações da infraestrutura local..."
    log_info "Creating .env file with local infrastructure settings..."
    
    cat > $ENV_FILE << 'EOF'
# =============================================================================
# QUICKELT ENVIRONMENT CONFIGURATION / CONFIGURAÇÃO DE AMBIENTE QUICKELT
# =============================================================================
# Configurações para infraestrutura local (Docker)
# Settings for local infrastructure (Docker)

# Database Configuration (PostgreSQL local via Docker)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USERNAME=quickelt_user
POSTGRES_PASSWORD=quickelt_password
POSTGRES_DATABASE=quickelt_db

# MinIO Configuration (S3-compatible local via Docker)
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123
AWS_REGION=us-east-1
AWS_S3_BUCKET=quickelt-data

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json

# Data Paths (local development)
DATA_PATH_BRONZE=/opt/quickelt/data/bronze
DATA_PATH_SILVER=/opt/quickelt/data/silver
DATA_PATH_GOLD=/opt/quickelt/data/gold

# =============================================================================
# CONFIGURAÇÕES OPCIONAIS / OPTIONAL CONFIGURATIONS
# =============================================================================
# Descomente e configure conforme necessário
# Uncomment and configure as needed

# MySQL Configuration
# MYSQL_HOST=localhost
# MYSQL_PORT=3306
# MYSQL_USERNAME=your_username
# MYSQL_PASSWORD=your_password
# MYSQL_DATABASE=your_database

# SharePoint Configuration
# AZURE_TENANT_ID=your_tenant_id
# AZURE_CLIENT_ID=your_client_id
# AZURE_CLIENT_SECRET=your_client_secret

# Production AWS S3 (comentar AWS_ENDPOINT_URL para usar AWS real)
# Production AWS S3 (comment AWS_ENDPOINT_URL to use real AWS)
# AWS_ACCESS_KEY_ID=your_aws_access_key
# AWS_SECRET_ACCESS_KEY=your_aws_secret_key
# AWS_REGION=us-east-1
# AWS_S3_BUCKET=your-production-bucket
EOF
    
    chmod 600 $ENV_FILE
    log_info "Arquivo .env criado com permissões seguras"
    log_info ".env file created with secure permissions"
}

# Configurar bucket MinIO / Set up MinIO bucket
setup_minio_bucket() {
    log_step "🪣 CONFIGURANDO BUCKET MINIO / SETTING UP MINIO BUCKET"
    
    # Instalar cliente MinIO / Install MinIO client
    if ! command_exists mc; then
        log_info "Instalando cliente MinIO (mc)..."
        log_info "Installing MinIO client (mc)..."
        wget https://dl.min.io/client/mc/release/linux-amd64/mc
        chmod +x mc
        sudo mv mc /usr/local/bin/
    fi
    
    # Aguardar MinIO estar pronto / Wait for MinIO to be ready
    log_info "Aguardando MinIO estar pronto..."
    log_info "Waiting for MinIO to be ready..."
    sleep 10
    
    # Configurar alias MinIO / Configure MinIO alias
    log_info "Configurando cliente MinIO..."
    log_info "Configuring MinIO client..."
    mc alias set local http://localhost:9000 minioadmin minioadmin123
    
    # Criar bucket se não existir / Create bucket if it doesn't exist
    local BUCKET_NAME="quickelt-data"
    if ! mc ls local/$BUCKET_NAME >/dev/null 2>&1; then
        log_info "Criando bucket: $BUCKET_NAME"
        log_info "Creating bucket: $BUCKET_NAME"
        mc mb local/$BUCKET_NAME
        
        # Criar estrutura de pastas / Create folder structure
        log_info "Criando estrutura de pastas no bucket..."
        log_info "Creating folder structure in bucket..."
        echo "bronze/" | mc pipe local/$BUCKET_NAME/bronze/.keep
        echo "silver/" | mc pipe local/$BUCKET_NAME/silver/.keep
        echo "gold/" | mc pipe local/$BUCKET_NAME/gold/.keep
    else
        log_info "Bucket $BUCKET_NAME já existe"
        log_info "Bucket $BUCKET_NAME already exists"
    fi
    
    # Listar buckets / List buckets
    log_info "Buckets disponíveis:"
    log_info "Available buckets:"
    mc ls local/
}

# Executar testes / Run tests
run_tests() {
    log_step "🧪 EXECUTANDO TESTES / RUNNING TESTS"
    
    cd /opt/quickelt
    
    # Ativar ambiente virtual / Activate virtual environment
    source .venv/bin/activate
    
    # Executar testes básicos / Run basic tests
    if [ -f "tests/test_ingestion_pandas.py" ]; then
        log_info "Executando testes básicos..."
        log_info "Running basic tests..."
        python -m pytest tests/ -v --tb=short
    else
        log_warn "Arquivos de teste não encontrados"
        log_warn "Test files not found"
    fi
}

# Criar script de inicialização / Create startup script
create_startup_script() {
    log_step "📜 CRIANDO SCRIPT DE INICIALIZAÇÃO / CREATING STARTUP SCRIPT"
    
    local STARTUP_SCRIPT="/opt/quickelt/start-quickelt.sh"
    
    cat > $STARTUP_SCRIPT << 'EOF'
#!/bin/bash
# Script para iniciar ambiente QuickELT
# Script to start QuickELT environment

echo "🚀 Iniciando ambiente QuickELT / Starting QuickELT environment"

# Navegar para diretório do projeto / Navigate to project directory
cd /opt/quickelt

# Iniciar infraestrutura Docker / Start Docker infrastructure
echo "📦 Iniciando infraestrutura Docker..."
echo "📦 Starting Docker infrastructure..."
cd infrastructure
docker-compose up -d

# Aguardar containers / Wait for containers
sleep 15

# Verificar status / Check status
echo "✅ Status da infraestrutura:"
echo "✅ Infrastructure status:"
docker-compose ps

# Voltar para diretório principal / Return to main directory
cd ..

# Ativar ambiente Python / Activate Python environment
echo "🐍 Ativando ambiente Python..."
echo "🐍 Activating Python environment..."
source .venv/bin/activate

echo ""
echo "🎉 Ambiente QuickELT pronto para uso!"
echo "🎉 QuickELT environment ready to use!"
echo ""
echo "📍 Acessos disponíveis / Available access:"
echo "   - PostgreSQL: localhost:5432 (user: quickelt_user, db: quickelt_db)"
echo "   - MinIO Console: http://localhost:9001 (user: minioadmin)"
echo "   - MinIO API: http://localhost:9000"
echo ""
echo "💡 Para executar testes: pytest tests/"
echo "💡 To run tests: pytest tests/"
echo "💡 Para parar infraestrutura: cd infrastructure && docker-compose down"
echo "💡 To stop infrastructure: cd infrastructure && docker-compose down"
EOF
    
    chmod +x $STARTUP_SCRIPT
    
    log_info "Script de inicialização criado: $STARTUP_SCRIPT"
    log_info "Startup script created: $STARTUP_SCRIPT"
}

# Função principal / Main function
main() {
    echo -e "${GREEN}"
    echo "=============================================================="
    echo "🚀 QUICKELT INFRASTRUCTURE SETUP"
    echo "   Configuração Automática de Infraestrutura"
    echo "   Automatic Infrastructure Setup"
    echo "=============================================================="
    echo -e "${NC}"
    
    # Verificar se está rodando como root / Check if running as root
    if [[ $EUID -eq 0 ]]; then
        log_error "Não execute este script como root!"
        log_error "Do not run this script as root!"
        exit 1
    fi
    
    # Verificar se tem sudo / Check if has sudo
    if ! sudo -n true 2>/dev/null; then
        log_info "Este script requer privilégios sudo"
        log_info "This script requires sudo privileges"
        sudo -v
    fi
    
    # Executar etapas / Execute steps
    check_os
    update_system
    install_docker
    install_docker_compose
    install_python
    setup_project_structure
    setup_python_environment
    setup_docker_infrastructure
    setup_environment_file
    setup_minio_bucket
    create_startup_script
    
    # Executar testes opcionalmente / Run tests optionally
    echo ""
    read -p "Deseja executar os testes agora? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        run_tests
    fi
    
    # Mensagem final / Final message
    log_step "🎉 CONFIGURAÇÃO CONCLUÍDA / SETUP COMPLETED"
    
    echo -e "${GREEN}✅ Infraestrutura QuickELT configurada com sucesso!${NC}"
    echo -e "${GREEN}✅ QuickELT infrastructure configured successfully!${NC}"
    echo ""
    echo -e "${BLUE}📍 Próximos passos / Next steps:${NC}"
    echo "1. Execute: source ~/.bashrc (para recarregar aliases / to reload aliases)"
    echo "2. Execute: /opt/quickelt/start-quickelt.sh (para iniciar ambiente / to start environment)"
    echo "3. Acesse MinIO Console: http://localhost:9001"
    echo "4. Configure credenciais adicionais no arquivo .env se necessário"
    echo "   Configure additional credentials in .env file if needed"
    echo ""
    echo -e "${YELLOW}⚠️  IMPORTANTE / IMPORTANT:${NC}"
    echo "- Faça logout e login novamente para aplicar as permissões do Docker"
    echo "- Log out and log in again to apply Docker permissions"
    echo "- O usuário foi adicionado ao grupo 'docker'"
    echo "- User was added to 'docker' group"
    echo ""
    echo -e "${GREEN}🚀 Happy coding with QuickELT!${NC}"
}

# Capturar sinais para limpeza / Catch signals for cleanup
trap 'log_error "Script interrompido pelo usuário"; exit 1' INT TERM

# Executar função principal / Execute main function
main "$@"