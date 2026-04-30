import { useEffect, useState, type FormEvent } from 'react'
import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { useUpdateCredentialSettings } from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import type { CredentialStatusItem } from '@/types/api'

interface CredentialSettingsDialogProps {
  credential: CredentialStatusItem | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function normalizeText(value: string): string | undefined {
  const trimmed = value.trim()
  return trimmed ? trimmed : undefined
}

export function CredentialSettingsDialog({
  credential,
  open,
  onOpenChange,
}: CredentialSettingsDialogProps) {
  const [priority, setPriority] = useState('0')
  const [authRegion, setAuthRegion] = useState('')
  const [apiRegion, setApiRegion] = useState('')
  const [machineId, setMachineId] = useState('')
  const [endpoint, setEndpoint] = useState('')
  const [proxyUrl, setProxyUrl] = useState('')
  const [proxyUsername, setProxyUsername] = useState('')
  const [proxyPassword, setProxyPassword] = useState('')
  const [clearProxyPassword, setClearProxyPassword] = useState(false)

  const updateSettings = useUpdateCredentialSettings()

  useEffect(() => {
    if (!credential || !open) return
    setPriority(String(credential.priority))
    setAuthRegion(credential.authRegion ?? '')
    setApiRegion(credential.apiRegion ?? '')
    setMachineId(credential.machineId ?? '')
    setEndpoint(credential.configuredEndpoint ?? '')
    setProxyUrl(credential.proxyUrl ?? '')
    setProxyUsername(credential.proxyUsername ?? '')
    setProxyPassword('')
    setClearProxyPassword(false)
  }, [credential, open])

  if (!credential) return null

  const handleSubmit = (event: FormEvent) => {
    event.preventDefault()
    const parsedPriority = Number.parseInt(priority, 10)
    if (!Number.isInteger(parsedPriority) || parsedPriority < 0) {
      toast.error('优先级必须是非负整数')
      return
    }

    updateSettings.mutate(
      {
        id: credential.id,
        payload: {
          priority: parsedPriority,
          authRegion: normalizeText(authRegion),
          apiRegion: normalizeText(apiRegion),
          machineId: normalizeText(machineId),
          endpoint: normalizeText(endpoint),
          proxyUrl: normalizeText(proxyUrl),
          proxyUsername: normalizeText(proxyUsername),
          proxyPassword: clearProxyPassword ? '' : normalizeText(proxyPassword),
        },
      },
      {
        onSuccess: (res) => {
          toast.success(res.message)
          setProxyPassword('')
          setClearProxyPassword(false)
          onOpenChange(false)
        },
        onError: (error: unknown) => {
          toast.error(`更新失败: ${extractErrorMessage(error)}`)
        },
      }
    )
  }

  const disabled = updateSettings.isPending

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>凭据设置 #{credential.id}</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="flex flex-col min-h-0 flex-1">
          <div className="space-y-4 py-4 overflow-y-auto flex-1 pr-1">
            <div className="space-y-2">
              <label htmlFor={`credential-${credential.id}-priority`} className="text-sm font-medium">
                优先级
              </label>
              <Input
                id={`credential-${credential.id}-priority`}
                type="number"
                min="0"
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
                disabled={disabled}
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Region</label>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <Input
                  placeholder="Auth Region"
                  value={authRegion}
                  onChange={(e) => setAuthRegion(e.target.value)}
                  disabled={disabled}
                />
                <Input
                  placeholder="API Region"
                  value={apiRegion}
                  onChange={(e) => setApiRegion(e.target.value)}
                  disabled={disabled}
                />
              </div>
            </div>

            <div className="space-y-2">
              <label htmlFor={`credential-${credential.id}-machine-id`} className="text-sm font-medium">
                Machine ID
              </label>
              <Input
                id={`credential-${credential.id}-machine-id`}
                value={machineId}
                onChange={(e) => setMachineId(e.target.value)}
                disabled={disabled}
              />
            </div>

            <div className="space-y-2">
              <label htmlFor={`credential-${credential.id}-endpoint`} className="text-sm font-medium">
                端点
              </label>
              <Input
                id={`credential-${credential.id}-endpoint`}
                placeholder="ide / cli"
                value={endpoint}
                onChange={(e) => setEndpoint(e.target.value)}
                disabled={disabled}
              />
            </div>

            <div className="space-y-2">
              <label htmlFor={`credential-${credential.id}-proxy-url`} className="text-sm font-medium">
                代理
              </label>
              <Input
                id={`credential-${credential.id}-proxy-url`}
                placeholder='代理 URL，"direct" 表示不用代理'
                value={proxyUrl}
                onChange={(e) => setProxyUrl(e.target.value)}
                disabled={disabled}
              />
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <Input
                  placeholder="代理用户名"
                  value={proxyUsername}
                  onChange={(e) => setProxyUsername(e.target.value)}
                  disabled={disabled}
                />
                <Input
                  type="password"
                  placeholder="新代理密码"
                  value={proxyPassword}
                  onChange={(e) => setProxyPassword(e.target.value)}
                  disabled={disabled || clearProxyPassword}
                />
              </div>
              <label className="flex items-center gap-2 text-sm text-muted-foreground">
                <Checkbox
                  checked={clearProxyPassword}
                  onCheckedChange={(checked) => setClearProxyPassword(checked === true)}
                  disabled={disabled}
                />
                清空代理密码
              </label>
            </div>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={disabled}
            >
              取消
            </Button>
            <Button type="submit" disabled={disabled}>
              {disabled ? '保存中...' : '保存'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
