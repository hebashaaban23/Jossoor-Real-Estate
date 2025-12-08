<template>
  <div class="rounded-xl bg-white shadow border p-4 flex flex-col gap-2">
    <div><slot name="header" /></div>
    <div><slot name="content" /></div>
    <div class="mt-2 flex justify-end"><slot name="footer" /></div>
  </div>
</template>
